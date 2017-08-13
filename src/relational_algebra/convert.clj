(ns relational-algebra.convert
  (:require [relational-algebra.core :refer :all])
  (:import [relational_algebra.core Select Join])
  )

(defn convert-select-commutative [sel]
  {:pre  [
          (instance? Select sel)
          (instance? Select (:tbl sel))
          ]}
  (let [
        sub-sel (:tbl sel)
        new-sub-sel (->Select (:tbl sub-sel) (:condition sel))
        new-sel (->Select (identity new-sub-sel) (:condition sub-sel))
        ]
    (identity new-sel))
  )

; (e1 join e2) join e3 -> e1 join (e2 join e3)
(defn convert-join-associative [join]
  {:pre  [
          (instance? Join join)
          (instance? Join (:left-tbl join))
          ]}
  (let [
        e1-join-e2 (:left-tbl join)
        e3 (:right-tbl join)
        e12-e3-cols (:col-matches join)
        e1 (:left-tbl e1-join-e2)
        e2 (:right-tbl e1-join-e2)
        e1-e2-cols (:col-matches e1-join-e2)
        e2-join-e3 (->Join e2 e3 e12-e3-cols)
        e1-join-e23 (->Join e1 e2-join-e3 e1-e2-cols)
        ]
    (identity e1-join-e23))
  )