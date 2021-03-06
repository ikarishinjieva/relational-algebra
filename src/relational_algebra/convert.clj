(ns relational-algebra.convert
  (:require 
    [relational-algebra.core :refer :all]
    [aprint.core :refer :all])
  (:import [relational_algebra.core Base Select Join Apply Project Aggregate])
  )

(defn convert-select-commutative [sel]
  {:pre  [
          (instance? Select sel)
          (instance? Select (:tbl sel))
          ]}
  (let [
        sub-sel (:tbl sel)
        new-sub-sel (->Select (:tbl sub-sel) (replace-tbl-on-fn-desc (:condition sel) {sub-sel (:tbl sub-sel)}))
        new-sel (->Select (identity new-sub-sel) (replace-tbl-on-fn-desc (:condition sub-sel) {(:tbl sub-sel) new-sub-sel}))
        ]
    (identity new-sel))
  )

(defn convert-base-to-select [base]
  {:pre  [
          (instance? Base base)
          ]}
  (->Select base `(:true)))

; (e1 join e2) join e3 -> e1 join (e2 join e3)
; TODO precondition: e12-e3-join-key is e1-e2-join-key??
(defn convert-join-associative [join]
  {:pre  [
          (instance? Join join)
          (instance? Join (:left-tbl join))
          ]}
  (let [
        e1-join-e2 (:left-tbl join)
        e3 (:right-tbl join)
        e12-e3-cols (:col-matches join)
        e12-e3-cols-e12 (key (first e12-e3-cols))
        e12-e3-cols-e3 (val (first e12-e3-cols))
        e1 (:left-tbl e1-join-e2)
        e2 (:right-tbl e1-join-e2)
        e1-e2-cols (:col-matches e1-join-e2)
        e1-e2-cols-e1 (key (first e1-e2-cols))
        e1-e2-cols-e2 (val (first e1-e2-cols))
        e2-join-e3 (->Join e2 e3 {(->Col e2 (:col e12-e3-cols-e12)) e12-e3-cols-e3} [] :inner)
        e1-join-e23 (->Join e1 e2-join-e3 {e1-e2-cols-e1 (->Col e2-join-e3 (:col e1-e2-cols-e2))} [] :inner)
        ]
    (identity e1-join-e23))
  )

; select(join(e1,e2)) -> theta-join(e1,e2)
(defn convert-select_join-to-theta_join [sel]
  {:pre  [
          (instance? Select sel)
          (instance? Join (:tbl sel))
          ]}
  (let [
        join (:tbl sel)
        new-join (->Join (:left-tbl join) (:right-tbl join) (:col-matches join) (:condition sel) :inner)
        ]
    (identity new-join))
  )

; select[c1](theta-join[c2](e1,e2)) -> theta-join[c1 and c2](e1,e2)
(defn convert-select_theta_join-to-theta_join [sel]
  {:pre  [
          (instance? Select sel)
          (instance? Join (:tbl sel))
          (has-cond? (:tbl sel))
          ]}
  (let [
        join (:tbl sel)
        sel-cond (:condition sel)
        join-cond (:condition join)
        new-join (->Join (:left-tbl join) (:right-tbl join) (:col-matches join) `(:and ~sel-cond ~join-cond) :inner)
        ]
    (identity new-join))
  )

; Rule 1 of <Orthogonal Optimization of Subqueries and Aggregation>
; Apply(R, E) -> Join(R, E) if E is not resolved from R
(defn convert-apply_whose_expr_not_resolved_from_relation-to-join [appl]
  {:pre  [
          (instance? Apply appl)
          (not (involve-tbl? (:expr appl) (:relation appl)))
          ]}
  (let [
        expr (:expr appl)
        rel (:relation appl)
        new-join (->Join rel expr {} [] :inner)
        ]
    (identity new-join))
  )
  
; Rule 2 of <Orthogonal Optimization of Subqueries and Aggregation>
; Apply(R, Select[p](E)) -> ThetaJoin[p](R, E)
(defn convert-apply_whose_select_expr_not_resolved_from_relation-to-theta_join [appl]
  {:pre  [
          (instance? Apply appl)
          (instance? Select (:expr appl))
          (not (involve-tbl? (:tbl (:expr appl)) (:relation appl)))
          ]}
  (let [
        sel (:expr appl)
        rel (:relation appl)
        new-join (->Join rel (:tbl sel) {} (:condition sel) (:join-type appl))
        ]
    (identity new-join))
  )

; Rule 3 of <Orthogonal Optimization of Subqueries and Aggregation>
; Apply(R, Select[p](E)) -> Select[p](Apply(R, E))
(defn convert-apply_select-to-select_apply [appl]
  {:pre  [
          (instance? Apply appl)
          (instance? Select (:expr appl))
          ]}
  (let [
        sel (:expr appl)
        rel (:relation appl)
        new-apply (make-Apply :relation rel, :expr (:tbl sel), :join-type (:join-type appl), :condition (:condition appl))
        new-sel (->Select new-apply (:condition sel))
        ]
    (identity new-sel))
  )

; Rule 4 of <Orthogonal Optimization of Subqueries and Aggregation>
; Apply(R, Project[p](E)) -> Project[p union R.cols](Apply(R, E))
(defn convert-apply_project-to-project_apply [appl]
  {:pre  [
          (instance? Apply appl)
          (instance? Project (:expr appl))
          ]}
  (let [
        proj (:expr appl)
        rel (:relation appl)
        new-apply (make-Apply :relation rel, :expr (:tbl proj), :join-type (:join-type appl), :condition (:condition appl)) 
        rel-cols (map #(->Col rel %) (meta-cols rel))
        new-cols-raw (apply conj (:cols proj) rel-cols)
        new-cols-with-replaced-tbl (replace-tbl-on-fn-desc new-cols-raw {
                                                                         (:tbl proj) new-apply
                                                                         rel new-apply
                                                                         })
        new-proj (->Project new-apply new-cols-with-replaced-tbl)
        ]
    (identity new-proj))
  )


; Rule 9 of <Orthogonal Optimization of Subqueries and Aggregation>
; Apply(R, Aggregate[null, F](E), Join) -> Aggregate[R.cols, F'](Apply(R, E, Left-Join))
(defn convert-apply_scalar_aggregate-to-vector_aggregate_apply [appl]
  {:pre  [
          (instance? Apply appl)
          (instance? Aggregate (:expr appl))
          (scalar-aggr? (:expr appl))
          (not (has-cond? (:expr appl)))
          ]}
  (let [
        rel (:relation appl)
        aggr (:expr appl)
        aggr-fn (:aggr-fn-desc aggr)
        aggr-tbl (:tbl aggr)
        new-apply (make-Apply :relation rel, :expr aggr-tbl, :join-type :left)
        new-group-cols (map #(->Col new-apply %) (meta-cols rel))
        new-aggr-fn-desc (replace-tbl-on-fn-desc (:aggr-fn-desc aggr) {aggr-tbl new-apply}) ;TODO: F, like count(*), should be convert to F', like count(col)
        new-cond (replace-tbl-on-fn-desc (:condition aggr) {aggr-tbl new-apply})
        new-aggr (->Aggregate new-apply new-group-cols new-aggr-fn-desc [])
        ]
    (identity new-aggr))
  )