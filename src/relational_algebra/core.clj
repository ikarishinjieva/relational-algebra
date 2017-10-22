(ns relational-algebra.core
  (:require [clojure.string :as str] 
            [clojure.set :as set] 
            [clojure.tools.logging :as log]
            [aprint.core :refer :all]))

(defmulti to-sql type)
(declare to-sub-sql)
(defmulti query (fn [rel data is-sub] (type rel)))
(declare query-sql)
(declare query-sub-sql)

(def table-name-seq (iterate inc 0))
(defn gen-table-name-seq [] (first (take 1 table-name-seq)))

(defprotocol IRelation
  (sql [this])
  (query-data [this data is-sub])
  (as-name [this])
  (replace-tbl [this tbl-mapping])
  (involve-tbl? [this matching-tbl])
  (meta-cols [this])
  (estimate-cost [this])
  (estimate-rows [this])
  (iterate-tbl [this iter-fn]))

(defprotocol ICondition
  (has-cond? [this]))

(defprotocol IAggregate
  (scalar-aggr? [this])
  (vector-aggr? [this]))

(defn make-tbl-prefix-mapping [cols tbl]
  (let [
      prefix (if (str/blank? tbl) "" (str tbl "."))
      map-fn #(list % 
                    (if (str/includes? % ".") 
                        (str/replace-first % #"[a-zA-Z0-9\_\-]+\." prefix)
                        (str/replace-first % #"([a-zA-Z0-9\_\-]+)" (str prefix "$1"))))
      ]
    (apply array-map (mapcat map-fn cols)))
)

(defn make-update-row-tbl-prefix-fn [new-tbl-name is-sub]
  (if is-sub
    (fn [row]
      (let [
           cols (keys row)
           new-cols-mapping (make-tbl-prefix-mapping cols new-tbl-name)
           ]
       (set/rename-keys row new-cols-mapping)
       ))
    identity
    ))

(defn update-row-tbl-prefix [relation is-sub raw-data]
  (let [
         new-tbl-name (as-name relation)
         ]
    (map (make-update-row-tbl-prefix-fn new-tbl-name is-sub) raw-data)))

(def sql-functions {
                    :> > 
                    :< <
                    :and (fn [a b] (and a b))
                    := =
                    :true (fn [] (identity true))
                    })

(defmulti cond-to-str (fn [cond is_nest] (type cond)))
(defmethod cond-to-str clojure.lang.Keyword [k is_nest] (name k))
(defmethod cond-to-str java.lang.String [k is_nest] (identity k))
(defmethod cond-to-str java.lang.Long [k is_nest] (identity k))

; TODO remove: cond-to-str clojure.lang.IPersistentList
(defmethod cond-to-str clojure.lang.IPersistentList [condition is_nest] 
  (if (empty? condition) "" 
    (let [
        op (to-sql (first condition))
        first-num (cond-to-str (nth condition 1) true)
        second-num (cond-to-str (nth condition 2) true)
        expr (str first-num " " op " " second-num)]
    (if is_nest (str "(" expr ")") expr))))

(defmethod cond-to-str clojure.lang.Cons [condition is_nest] 
  (let [
        op (to-sql (first condition))
        first-num (cond-to-str (nth condition 1) true)
        second-num (cond-to-str (nth condition 2) true)
        expr (str first-num " " op " " second-num)]
    (if is_nest (str "(" expr ")") expr)))

(defmethod cond-to-str clojure.lang.LazySeq [condition is_nest]
  (cond-to-str (apply list condition) is_nest))

(defmethod cond-to-str clojure.lang.PersistentVector [condition is_nest]
  (cond-to-str (apply list condition) is_nest))

; MockTbl is internal-use for Table replace
(defrecord MockTbl [mock-data]
  IRelation
  (query-data [this data is-sub]
              (identity mock-data))
  (as-name [_] 
         (str "mock" (gen-table-name-seq)))
  (estimate-cost [this] (identity 1))
  (estimate-rows [this] (identity 1))
  (iterate-tbl [this iter-fn] (identity [(iter-fn this)])))

(defn replace-col-name-table-prefix [col-name new-tbl-prefix]
  (let [
    sep (if (empty? new-tbl-prefix) "" ".")
    ]
    (str/replace col-name #"[a-zA-Z0-9\_\-]+\." (str new-tbl-prefix sep))))

(defrecord Col [tbl col]
  IRelation
  (sql [_] 
    (let [col-without-tbl (replace-col-name-table-prefix col "")]
      (if (str/includes? col-without-tbl "(") 
        (str/replace-first col-without-tbl #"\(" (str "(" (as-name tbl) "."))
        (str (as-name tbl) "." col-without-tbl))))
  (query-data [this row _] 
        (let [
              r (if (instance? MockTbl tbl) 
                  (first (query-sub-sql tbl row)) ;presume MockTbl has only one row
                  row)
              update-prefix-fn (make-update-row-tbl-prefix-fn (as-name tbl) true)
              updated-prefix-row (update-prefix-fn r)
              col-name (sql this)
              ]
          (or 
            (get row col-name)
            (get updated-prefix-row col-name))))
  (replace-tbl [this tbl-mapping]
               (get tbl-mapping this 
                 (let [new-tbl (replace-tbl tbl tbl-mapping)]
                     (->Col new-tbl col))))
  (involve-tbl? [this matching-tbl]
                (some true? [
                             (= this matching-tbl)
                             (involve-tbl? tbl matching-tbl)
                             ]))
  (meta-cols [this] (identity col))
  (estimate-cost [this] (identity 1))
  (estimate-rows [this] (estimate-rows tbl))
  (iterate-tbl [this iter-fn] 
               (concat 
                 [(iter-fn this)] 
                 (iterate-tbl tbl iter-fn))))

(defmethod cond-to-str Col [col is_nest] 
  (sql col))

(defn col-matches-to-str [col-matches]
  (str/join " " (map #(str (to-sql (first %)) " = " (to-sql (second %))) col-matches)))

(defn update-map-kv [m f & args]
 (reduce (fn [r [k v]] (assoc r (apply f k args) (apply f v args))) {} m))

(defn update-map-k [m f & args]
 (reduce (fn [r [k v]] (assoc r (apply f k args) v)) {} m))

(defn remove-data-table-prefix [rows]
  (let [
        remove-prefix-fn (fn [k] (replace-col-name-table-prefix k ""))
        ]
    (map #(update-map-k %1 remove-prefix-fn) rows)))

(defn replace-tbl-on-fn-desc [condition tbl-mapping]
  (sequence (map 
    #(if (satisfies? IRelation %1) 
       (replace-tbl %1 tbl-mapping)
       %1)
    condition)))

(defn involve-tbl-on-fn-desc? [condition matching-tbl]
  (some true? (map 
    #(if (satisfies? IRelation %1) 
       (involve-tbl? %1 matching-tbl)
       false)
    condition)))

(defn array-index
  "similar to set/index, except `array-index` is stable on sequence"
  [xrel ks]
    (reduce
     (fn [m x]
       (let [ik (select-keys x ks)]
         (assoc m ik (conj (get m ik []) x))))
     {} xrel))

(defrecord Base [tbl cols]
  IRelation
  (sql [this] 
       (name tbl))
  (query-data [this data is-sub] 
         (let [
               raw-data (get data tbl)
               ]
           (update-row-tbl-prefix this is-sub raw-data)))
  (as-name [_] 
         (str (name tbl) (gen-table-name-seq)))
  (replace-tbl [this tbl-mapping]
               (get tbl-mapping this this))
  (involve-tbl? [this matching-tbl]
                (= this matching-tbl))
  (meta-cols [this] (identity cols))
  (estimate-cost [this] (identity 1)) 
  (estimate-rows [this] (identity 1024)) ;presume a table has 1024 rows
  (iterate-tbl [this iter-fn] (identity [(iter-fn this)]))
)

(defrecord Project [tbl cols]
  IRelation
  (sql [_] 
       (let [cols-str (str/join ", " (map to-sql cols))]
         (str "SELECT " cols-str " FROM " (to-sub-sql tbl))
         ))
  (query-data [this data is-sub] 
         (let [
               tbl-data (query-sub-sql tbl data)
               col-str-match-col-fn (fn [col col-str] (= col-str (sql col)))
               filter-col-fn (fn [kv] (let [col-str (first kv)]
                                        (some #(col-str-match-col-fn % col-str) cols)))
               filter-row-fn (fn [row] 
                               (apply array-map (flatten (filter filter-col-fn row))))
               raw-data (map filter-row-fn tbl-data)
               ]
           (update-row-tbl-prefix this is-sub raw-data)))
  (as-name [_] 
         (as-name tbl))
  (replace-tbl [this tbl-mapping]
               (get tbl-mapping this 
                    (let [new-tbl (replace-tbl tbl tbl-mapping)]
                      (->Project new-tbl cols))))
  (involve-tbl? [this matching-tbl]
                (some true? [
                             (= this matching-tbl)
                             (involve-tbl? tbl matching-tbl)
                             ]))
  (meta-cols [this] (map :col cols))
  (estimate-cost [this] (+ (estimate-cost tbl) 1))
  (estimate-rows [this] (estimate-rows tbl))
  (iterate-tbl [this iter-fn] 
               (concat 
                [(iter-fn this)]
                (iterate-tbl tbl iter-fn))))

(defrecord Select [tbl condition]
  IRelation
  (sql [_]
       (let 
         [
          cond-str (cond-to-str condition false)
          tbl-str (to-sub-sql tbl)]
         (str "SELECT * FROM " tbl-str " WHERE " cond-str)
         ))
  (query-data [this data is-sub] 
         (let 
           [tbl-data (query-sub-sql tbl data)
            raw-data (filter #(query-sub-sql condition %1) tbl-data)
            ]
           (update-row-tbl-prefix this is-sub raw-data)))
  (as-name [_] 
         (str "s" (gen-table-name-seq)))
  (replace-tbl [this tbl-mapping]
               (get tbl-mapping this 
                 (let [
                       new-tbl (replace-tbl tbl tbl-mapping)
                       new-condition (replace-tbl-on-fn-desc condition tbl-mapping)
                       ]
                   (->Select new-tbl new-condition))))
  (involve-tbl? [this matching-tbl]
                (some true? [
                      (= this matching-tbl)
                      (involve-tbl? tbl matching-tbl)
                      (involve-tbl-on-fn-desc? condition matching-tbl)
                      ]))
  (meta-cols [this] (meta-cols tbl))
  (estimate-cost [this] (+ (estimate-cost tbl) (estimate-rows tbl)))
  (estimate-rows [this] (if (= [:true] condition) 
    (estimate-rows tbl)
    (/ (estimate-rows tbl) 4) ;presume condition matches 1/4 rows
    )) 
  (iterate-tbl [this iter-fn]
               (concat  
                [(iter-fn this)]
                (iterate-tbl tbl iter-fn)))
)

(defrecord Join [left-tbl right-tbl col-matches condition join-type]
  ICondition
  (has-cond? [this]
             (not (empty? condition)))
  IRelation
  (sql [this] 
       (let [
             left-tbl-str (to-sub-sql left-tbl)
             right-tbl-str (to-sub-sql right-tbl)
             col-str (if (empty? col-matches) "" (str  " ON " (col-matches-to-str col-matches)))
             cond-str (if (has-cond? this) (str " WHERE " (cond-to-str condition false)))
             join-word ((:join-type this) {:inner "JOIN", :left "LEFT JOIN"})
             join-str (str "SELECT * FROM " left-tbl-str " " join-word " " right-tbl-str)
             ]
         (str join-str col-str cond-str)))
  (query-data [this data is-sub] 
         (let 
           [
            left-tbl-data (query-sub-sql left-tbl data)
            left-tbl-col-names (map sql (keys col-matches))
            right-tbl-data (query-sub-sql right-tbl data)
            right-tbl-col-names (map sql (vals col-matches))
            right-tbl-data-indexes (array-index right-tbl-data right-tbl-col-names)
            col-mapping (zipmap left-tbl-col-names right-tbl-col-names)
            joined-data (reduce (fn [ret row-in-left-tbl]
                     (let [
                           left-row-in-right-key (set/rename-keys (select-keys row-in-left-tbl left-tbl-col-names) col-mapping)
                           join-rows-in-right-tbl (or (get right-tbl-data-indexes left-row-in-right-key) [])
                           reduce-fn-if-row-match-cond (fn [ret row-in-right-tbl] 
                                                         (let [new-row (merge row-in-right-tbl row-in-left-tbl)]
                                                           (if (query-sub-sql condition new-row) 
                                                             (conj ret new-row) 
                                                             ret)))
                           joined-rows (reduce reduce-fn-if-row-match-cond [] join-rows-in-right-tbl)
                           ]
                         (if-not (empty? joined-rows)
                           (apply conj ret joined-rows)
                           (if (= (:join-type this) :left)
                             (conj ret row-in-left-tbl)
                             ret)
                         ))) [] left-tbl-data)
            ]
           (update-row-tbl-prefix this is-sub joined-data)))
  (as-name [_] 
         (str "j" (gen-table-name-seq)))
  (replace-tbl [this tbl-mapping]
               (get tbl-mapping this 
                 (let [
                       new-left-tbl (replace-tbl left-tbl tbl-mapping)
                       new-right-tbl (replace-tbl right-tbl tbl-mapping)
                       new-col-matches (update-map-kv col-matches replace-tbl tbl-mapping)
                       new-condition (replace-tbl-on-fn-desc condition tbl-mapping)
                       ]
                     (->Join new-left-tbl new-right-tbl new-col-matches new-condition (:join-type this)))))
  (involve-tbl? [this matching-tbl]
                (some true? [
                      (= this matching-tbl)
                      (involve-tbl? left-tbl matching-tbl)
                      (involve-tbl? right-tbl matching-tbl)
                      (involve-tbl-on-fn-desc? condition matching-tbl)
                      ]))
  (meta-cols [this] (apply conj (meta-cols left-tbl) (meta-cols right-tbl)))
  (estimate-cost [this] (let [join-cost (+ (estimate-rows left-tbl) (estimate-rows right-tbl))]
                   (+ (estimate-cost left-tbl) (estimate-cost right-tbl) join-cost)))
  (estimate-rows [this] (let [
                              join-rows (* (estimate-rows left-tbl) (estimate-rows right-tbl))
                              matches-devision (if (has-cond? this) 4 1)
                              ]
                          (/ join-rows matches-devision)))
  (iterate-tbl [this iter-fn] 
               (concat 
                [(iter-fn this)]
                (iterate-tbl left-tbl iter-fn)
                (iterate-tbl right-tbl iter-fn))))

(defn aggr-avg [items key] 
  (if (empty? items)
    0
    (let [
          values (replace {nil 0} (map #(query-sub-sql key %) items))
          ]
      (/ (apply + values) (count values)))))

(defn aggr-sum [items key] 
  (let [
        values (replace {nil 0} (map #(query-sub-sql key %) items))
        ]
    (apply + values)))

(def aggr-functions {
                     :avg aggr-avg
                     :sum aggr-sum})

(defrecord Aggregate [tbl group-cols aggr-fn-desc condition]
  IAggregate
  (scalar-aggr? [this] (empty? group-cols))
  (vector-aggr? [this] (not (scalar-aggr? this)))
  
  ICondition
  (has-cond? [this] (not (empty? condition)))
  
  IRelation
  (sql [_]
       (let 
         [
          aggr-fn (first aggr-fn-desc)
          aggr-fn-arg (first (rest aggr-fn-desc)) ; presume aggr-fn has 1 argument
          aggr-fn-str (str (to-sql aggr-fn) "(" (to-sql aggr-fn-arg) ")")
          has-groupby (not (empty? group-cols))
          group-cols-str (str/join ", " (map to-sql group-cols))
          cols-str (if has-groupby (str/join ", " [group-cols-str aggr-fn-str]) aggr-fn-str)
          tbl-str (to-sub-sql tbl)
          has-cond (not (empty? condition))
          cond-str (cond-to-str condition false)
          base-sql (str "SELECT " cols-str " FROM " tbl-str)
          group-by-sql (if has-groupby (str base-sql " GROUP BY " group-cols-str) base-sql)
          cond-sql (if has-cond (str group-by-sql " WHERE " cond-str) group-by-sql)
          ]
         (identity cond-sql)
         ))
  (query-data [this data is-sub] 
         (let 
           [
            tbl-data (query-sub-sql tbl data)
            has-cond (not (empty? condition))
            filter-fn (if has-cond
                       #(query-sub-sql condition %1)
                       identity)
            tbl-data-filtered (filter filter-fn tbl-data)
            tbl-data-by-group (if (empty? group-cols)
                                {{} tbl-data-filtered}
                                (let [
                                      group-cols-names (map sql group-cols)
                                      ]
                                  (set/index tbl-data-filtered group-cols-names)))
                                
            aggr-fn-name (first aggr-fn-desc)
            aggr-fn (aggr-fn-name aggr-functions)
            aggr-fn-arg (first (rest aggr-fn-desc)) ; presume aggr-fn has 1 argument
            aggregate (fn [group-keys group-items] (let [
                                                         aggred (aggr-fn group-items aggr-fn-arg)
                                                         aggred-key (str (name aggr-fn-name) "(" (sql aggr-fn-arg) ")")
                                                         ]
                                                     (conj group-keys {aggred-key aggred})))
            aggred-data (map #(apply aggregate %) tbl-data-by-group)
            ]
           (update-row-tbl-prefix this is-sub aggred-data)))
  (as-name [_] 
         (str "a" (gen-table-name-seq)))
  (replace-tbl [this tbl-mapping]
               (get tbl-mapping this 
                 (let [
                       new-tbl (replace-tbl tbl tbl-mapping)
                       new-group-cols (map #(replace-tbl %1 tbl-mapping) group-cols)
                       new-aggr-fn-desc (replace-tbl-on-fn-desc aggr-fn-desc tbl-mapping)
                       new-condition (replace-tbl-on-fn-desc condition tbl-mapping)
                       ]
                     (->Aggregate new-tbl new-group-cols new-aggr-fn-desc new-condition))))
  (involve-tbl? [this matching-tbl]
                (some true? [
                      (= this matching-tbl)
                      (involve-tbl? tbl matching-tbl)
                      (involve-tbl-on-fn-desc? aggr-fn-desc matching-tbl)
                      (involve-tbl-on-fn-desc? condition matching-tbl)
                      ]))
  (meta-cols [this] (let [
                          aggr-fn (first aggr-fn-desc)
                          aggr-fn-arg (first (rest aggr-fn-desc)) ; presume aggr-fn has 1 argument
                          aggr-fn-str (str (to-sql aggr-fn) "(" (to-sql aggr-fn-arg) ")")
                          ]
                      (conj group-cols aggr-fn-str)))
  (estimate-cost [this] (+ (estimate-cost tbl) (estimate-rows tbl)))
  (estimate-rows [this] (/ (estimate-rows tbl) 8)) ;presume aggregate rows is 1/8 of rows
  (iterate-tbl [this iter-fn] 
               (concat 
                [(iter-fn this)]
                (iterate-tbl tbl iter-fn)))
)

;TODO make it for all Relation
;DONOT add "as" clause when Relation is used as a condition variable (like Scalar Aggregation)
(defmethod cond-to-str Aggregate [rel is_nest] (str "(" (to-sql rel) ")")) 

; Apply = { foreach (rel in relation) { return op(rel, expr(rel)) } }
; Apply is with `condition` and is treated as Select(Apply), because pushing Select condition down is a little complicated.
(defrecord Apply [relation expr join-type condition]
  IRelation
  (sql [_]
    (if (empty? condition) 
      (str "THIS_IS_APPLY_CANNOT_CONVERT_TO_SQL")
      (str "SELECT * from " (to-sub-sql relation) " WHERE " (cond-to-str condition false))))
  (query-data [this data is-sub]
         (let
           [
            expr-col (first (meta-cols expr))
            replaced-condition (replace-tbl-on-fn-desc condition {expr (->Col expr expr-col)})
            apply-expr (fn [row] (let [
                                       joined (->Join relation expr {} replaced-condition join-type)
                                       applied-joined (replace-tbl joined {relation (->MockTbl [row])})
                                       ]
                                   (query-sub-sql applied-joined data)))
            relation-data (query-sub-sql relation data)
            applied-data (into [] (reduce concat [] (map apply-expr relation-data)))
            ]
           (update-row-tbl-prefix this is-sub applied-data)
           ))
  (as-name [_] 
         (str "ap" (gen-table-name-seq)))
  (replace-tbl [this tbl-mapping]
               (get tbl-mapping this 
                 (let [
                       new-relation (replace-tbl relation tbl-mapping)
                       new-expr (replace-tbl expr tbl-mapping)
                       new-condition (replace-tbl-on-fn-desc condition tbl-mapping)
                       ]
                     (->Apply new-relation new-expr join-type new-condition))))
  (involve-tbl? [this matching-tbl]
                (some true? [
                      (= this matching-tbl)
                      (involve-tbl? relation matching-tbl)
                      (involve-tbl? expr matching-tbl)
                      (involve-tbl-on-fn-desc? condition matching-tbl)
                      ]))
  (meta-cols [this] (apply conj (meta-cols relation) (meta-cols expr)))
  (estimate-cost [this] (+ 
                          (estimate-cost relation) 
                          (* 
                            (estimate-rows relation) 
                            (estimate-cost expr) ;expr is established by every relation row, so use estimate-cost insteand of estimate-rows
                            ))) 
  (estimate-rows [this] (/ (* (estimate-rows relation) (estimate-rows expr)) 8)) ;presume aggregate rows is 1/8 of rows
  (iterate-tbl [this iter-fn]
               (concat  
                [(iter-fn this)]
                (iterate-tbl relation iter-fn)
                (iterate-tbl expr iter-fn)))
)

(defn make-Apply
  [& {:keys [relation expr join-type condition] :or {join-type :inner condition '()}}] 
  (->Apply relation expr join-type condition))

(defmethod to-sql clojure.lang.Keyword [k] (name k))
(defmethod to-sql java.lang.Long [long] (str long))
(defmethod to-sql Base [base] (sql base))
(defmethod to-sql Project [project] (sql project))
(defmethod to-sql Select [select] (sql select))
(defmethod to-sql Join [join] (sql join))
(defmethod to-sql Aggregate [aggr] (sql aggr))
(defmethod to-sql Col [col] (sql col))
(defmethod to-sql Apply [apply] (sql apply))
(defmethod to-sql java.lang.String [str] (identity str))

(defn to-sub-sql [a]
  (let [sql (to-sql a)]
    (cond
      (instance? Base a) (str sql " AS " (as-name a))
      (str/includes? sql " ") (str "(" sql ") AS " (as-name a))
      :else sql
      )
    ))

(defmethod query clojure.lang.Keyword [k row _] (k row))
(defmethod query java.lang.Long [long row _] (identity long))
(defmethod query Base [base data is-sub] (query-data base data is-sub))
(defmethod query Project [project data is-sub] (query-data project data is-sub))
(defmethod query Select [sel data is-sub] (query-data sel data is-sub))
(defmethod query Join [join data is-sub] (query-data join data is-sub))
(defmethod query Aggregate [aggr data is-sub] (query-data aggr data is-sub))
(defmethod query Apply [apply data is-sub] (query-data apply data is-sub))
(defmethod query Col [col data is-sub] (query-data col data is-sub))
(defmethod query java.lang.String [str _ _] (identity str))
(defmethod query MockTbl [tbl data is-sub] (query-data tbl data is-sub))
(defmethod query clojure.lang.PersistentList$EmptyList [condition row is-sub]  ;condition
  (identity true))
(defmethod query clojure.lang.LazySeq [condition row is-sub]  ;condition
  (query (apply list condition) row is-sub))
(defmethod query clojure.lang.Cons [condition row is-sub]  ;condition
  (query (apply list condition) row is-sub))
(defmethod query clojure.lang.PersistentVector [condition row is-sub]  ;condition
  (query (apply list condition) row is-sub))
(defmethod query clojure.lang.PersistentList [condition row _] ;condition
  (if (empty? condition) true
    (let [
        cond-fn-name (first condition)
        cond-fn (cond-fn-name sql-functions)
        cond-args (rest condition)
        queried-args (map #(query-sub-sql % row) cond-args)
        ]
        (log/debugf "query condition: (%s %s), row: %s" cond-fn-name (pr-str queried-args) (pr-str row))
        (apply cond-fn queried-args))))

(defn query-sql [op data]
  (log/debugf "query (%s)" (type op))
  (query op data false))
(defn query-sub-sql [op data]
  (log/debugf "sub-query (%s)" (type op))
  (query op data true))

(defn eq [relA relB]
  (= (as-name relA) (as-name relB)))