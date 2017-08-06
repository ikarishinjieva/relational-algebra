(ns relational-algebra.convert
  (:require [relational-algebra.core :refer :all])
  (:import [relational_algebra.core Select])
  )

(defn convert-select-cascade [sel]
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