(ns relational-algebra.core-test
  (:require [clojure.test :refer :all]
            [relational-algebra.core :refer :all]))

(def person (->Base :tbl_person))

(def data {
           :tbl_person '(
                          {:id 1 :name "alex"}
                          {:id 2 :name "alexon"}
                          {:id 3 :name "richard"}
                          )})

(deftest to-sql-base
  (let [
        actual (to-sql person)
        expect "tbl_person"] 
    (is (= expect actual))
    ))

(deftest to-sql-project
  (let [
        proj (->Project person '(:id :name))
        actual (to-sql proj)
        expect "SELECT id, name FROM tbl_person"] 
    (is (= expect actual))
    ))

(deftest to-sql-select
  (let [
        proj (->Select person '(:> :id 10))
        actual (to-sql proj)
        expect "SELECT * FROM tbl_person WHERE id > 10"] 
    (is (= expect actual))
    ))

(deftest query-sql-base
  (let [
        actual (query-sql person data)
        expect (:tbl_person data)] 
    (is (= expect actual))
    ))

(deftest query-sql-project
  (let [
        proj (->Project person '(:id))
        actual (query-sql proj data)
        expect '({:id 1} {:id 2} {:id 3})] 
    (is (= expect actual))
    ))