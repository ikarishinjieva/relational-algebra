(ns relational-algebra.convert
  (:require [relational-algebra.core :refer :all])
  (:import [relational_algebra.core Select Join ThetaJoin Apply])
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
        e2-join-e3 (->Join e2 e3 {(->Col e2 (:col e12-e3-cols-e12)) e12-e3-cols-e3})
        e1-join-e23 (->Join e1 e2-join-e3 {e1-e2-cols-e1 (->Col e2-join-e3 (:col e1-e2-cols-e2))})
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
        new-join (->ThetaJoin (:left-tbl join) (:right-tbl join) (:col-matches join) (:condition sel))
        ]
    (identity new-join))
  )

; select[c1](theta-join[c2](e1,e2)) -> theta-join[c1 and c2](e1,e2)
(defn convert-select_theta_join-to-theta_join [sel]
  {:pre  [
          (instance? Select sel)
          (instance? ThetaJoin (:tbl sel))
          ]}
  (let [
        join (:tbl sel)
        sel-cond (:condition sel)
        join-cond (:condition join)
        new-join (->ThetaJoin (:left-tbl join) (:right-tbl join) (:col-matches join) `(:and ~sel-cond ~join-cond))
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
        new-join (->Join rel expr {})
        ]
    (identity new-join))
  )
  
; Rule 2 of <Orthogonal Optimization of Subqueries and Aggregation>
; Apply(R, Select[p](E)) -> ThetaJoin[p](R, E)
(defn convert-apply_whose_select_expr_not_resolved_from_relation-to-theta_join [appl]
  {:pre  [
          (instance? Apply appl)
          (instance? Select (:expr appl))
          ]}
  (let [
        sel (:expr appl)
        rel (:relation appl)
        new-join (->ThetaJoin rel (:tbl sel) {} (:condition sel))
        ]
    (identity new-join))
  )