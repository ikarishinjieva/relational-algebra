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
        p (->Base :tbl_person)
        proj (->Project p (list (->Col p :id) (->Col p :name)))
        actual (to-sql proj)
        expect "SELECT tbl_person0.id, tbl_person0.name FROM tbl_person AS tbl_person0"] 
    (is (= expect actual))
    ))

(deftest to-sql-select
  (let [
        sel (->Select person '(:> :id 10))
        actual (to-sql sel)
        expect "SELECT * FROM tbl_person AS tbl_person0 WHERE id > 10"] 
    (is (= expect actual))
    ))

(deftest to-sql-select-select
  (let [
        sel (->Select (->Select person '(:> :id 10)) '(:< :id 30))
        actual (to-sql sel)
        expect "SELECT * FROM (SELECT * FROM tbl_person AS tbl_person0 WHERE id > 10) AS s0 WHERE id < 30"] 
    (is (= expect actual))
    ))

(deftest to-sql-select-and
  (let [
        sel (->Select person '(:and (:> :id 10) (:< :id 30)))
        actual (to-sql sel)
        expect "SELECT * FROM tbl_person AS tbl_person0 WHERE (id > 10) and (id < 30)"] 
    (is (= expect actual))
    ))

(deftest to-sql-join
  (let [
        sel (->Join person city {:city :city_code})
        actual (to-sql sel)
        expect "SELECT * FROM tbl_person AS tbl_person0 JOIN tbl_city AS tbl_city0 ON city = city_code"]
    (is (= expect actual))
    ))

(deftest to-sql-theta-join
  (let [
        sel (->ThetaJoin person city {:city :city_code} '(:> :id 2))
        actual (to-sql sel)
        expect "SELECT * FROM tbl_person AS tbl_person0 JOIN tbl_city AS tbl_city0 ON city = city_code WHERE id > 2"] 
    (is (= expect actual))
    ))

(deftest to-sql-vector-aggregate
  (let [
        aggr (->Aggregate person [:city] '(:avg :age) '())
        actual (to-sql aggr)
        expect "SELECT city, avg(age) FROM tbl_person GROUP BY city"] 
    (is (= expect actual))
    ))

(deftest to-sql-scalar-aggregate
  (let [
        aggr (->Aggregate person [] '(:avg :age) '(:> :id 2))
        actual (to-sql aggr)
        expect "SELECT avg(age) FROM tbl_person WHERE id > 2"] 
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

(deftest query-sql-select-select
  (let [
        sel (->Select (->Select person '(:> :id 1)) '(:< :id 3))
        actual (query-sql sel data)
        expect [{:id 2 :name "alexon" :city "BJ" :age 30}]] 
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

(deftest query-sql-theta-join
  (let [
        sel (->ThetaJoin person city {:city :city_code} '(:> :id 2))
        actual (query-sql sel data)
        expect [
                {:city_code "SH", :city_name "ShangHai", :id 3, :name "richard", :city "SH" :age 28}
                ]] 
    (is (= expect actual))
    ))

(deftest query-sql-aggregate
  (let [
        aggr (->Aggregate person [:city] '(:avg :age) '())
        actual (query-sql aggr data)
        aggr-col-name (keyword "avg(age)")
        expect [{:city "SH", aggr-col-name 32} {:city "BJ", aggr-col-name 30}]
        ]
    (is (= expect actual))
    ))

(deftest query-sql-apply-whose-relation-and-expr-has-no-relation
  (let [
        aggr (->Aggregate person [] '(:avg :age) '(:> :id 2))
        appl (->Apply city aggr)
        actual (query-sql appl data)
        aggr-col-name (keyword "avg(age)")
        expect [{aggr-col-name 94/3, :city_code "SH", :city_name "ShangHai"} {aggr-col-name 94/3, :city_code "BJ", :city_name "BeiJing"}]
        ]
    (is (= expect actual))
    ))
