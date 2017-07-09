(ns relational-algebra.core-test
  (:require [clojure.test :refer :all]
            [relational-algebra.core :refer :all]))

(defrelation person :tbl_person)

(def data {:tbl_person '(2 3 4 5)})

(deftest to-sql-base
  (let [
        actual (to-sql person)
        expect "SELECT * FROM tbl_person"] 
    (is (= expect actual))
    ))

(deftest query-sql-base
  (let [
        actual (query-sql person data)
        expect (get data :tbl_person)] 
    (is (= expect actual))
    ))
