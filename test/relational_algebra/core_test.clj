(ns relational-algebra.core-test
  (:require [clojure.test :refer :all]
            [relational-algebra.core :refer :all]))

(def person (->Base :tbl_person))

(def city (->Base :tbl_city))

(def data {
           :tbl_person [
                        {:id 1 :name "alex" :city "SH" :age 36}
                        {:id 2 :name "alexon" :city "BJ" :age 30}
                        {:id 3 :name "richard" :city "SH" :age 28}
                        ]
           :tbl_city [
                      {:city_code "SH" :city_name "ShangHai"}
                      {:city_code "BJ" :city_name "BeiJing"}
                      ]
           })

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
        sel (->Select person '(:> :id 10))
        actual (to-sql sel)
        expect "SELECT * FROM tbl_person WHERE id > 10"] 
    (is (= expect actual))
    ))

(deftest to-sql-join
  (let [
        sel (->Join person city {:city :code})
        actual (to-sql sel)
        expect "SELECT * FROM tbl_person JOIN tbl_city ON city = code"] 
    (is (= expect actual))
    ))

(deftest to-sql-aggregate
  (let [
        aggr (->Aggregate person [:city] '(:avg :age))
        actual (to-sql aggr)
        expect "SELECT city, avg(age) FROM tbl_person GROUP BY city"] 
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
        expect [{:id 1} {:id 2} {:id 3}]] 
    (is (= expect actual))
    ))

(deftest query-sql-select
  (let [
        sel (->Select person '(:> :id 2))
        actual (query-sql sel data)
        expect [{:id 3 :name "richard" :city "SH" :age 28}]] 
    (is (= expect actual))
    ))

(deftest query-sql-join
  (let [
        sel (->Join person city {:city :city_code})
        actual (query-sql sel data)
        expect [
                {:city_code "SH", :city_name "ShangHai", :id 1, :name "alex", :city "SH" :age 36} 
                {:city_code "BJ", :city_name "BeiJing", :id 2, :name "alexon", :city "BJ" :age 30} 
                {:city_code "SH", :city_name "ShangHai", :id 3, :name "richard", :city "SH" :age 28}
                ]] 
    (is (= expect actual))
    ))

(deftest query-sql-aggregate
  (let [
        aggr (->Aggregate person [:city] '(:avg :age))
        actual (query-sql aggr data)
        expect [{:city "SH", :age 32} {:city "BJ", :age 30}]
        ]
    (is (= expect actual))
    ))