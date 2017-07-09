(ns relational-algebra.core-test
  (:require [clojure.test :refer :all]
            [relational-algebra.core :refer :all]))

(defrelation person :tbl_person)

(def data {:tbl_person 
           '(
             {:id 1, :name "alex"}
             {:id 2, :name "alexon"}
             )})

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

(deftest to-sql-projection
  (let [
        actual (to-sql (project person '[:id :name]))
        expect "SELECT id, name FROM tbl_person"] 
    (is (= expect actual))
    ))

(deftest query-sql-projection
  (let [
        actual (query-sql (project person '[:id]) data)
        expect '({:id 1} {:id 2})] 
    (is (= expect actual))
    ))