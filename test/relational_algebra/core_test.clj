(ns relational-algebra.core-test
  (:require [clojure.test :refer :all]
            [relational-algebra.core :refer :all]))

(def person (->Base :tbl_person))

(def city (->Base :tbl_city))

(def data {
           :tbl_person [
                        {"id" 1, "name" "alex", "city" "SH", "age" 36}
                        {"id" 2, "name" "alexon", "city" "BJ", "age" 30}
                        {"id" 3, "name" "richard", "city" "SH", "age" 28}
                        ]
           :tbl_city [
                      {"city_code" "SH", "city_name" "ShangHai"}
                      {"city_code" "BJ", "city_name" "BeiJing"}
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
        proj (->Project p `(~(->Col p "id") ~(->Col p "name")))
        actual (to-sql proj)
        expect "SELECT tbl_person0.id, tbl_person0.name FROM tbl_person AS tbl_person0"] 
    (is (= expect actual))
    ))

(deftest to-sql-select
  (let [
        p (->Base :tbl_person)
        sel (->Select p `(:> ~(->Col p "id") 10))
        actual (to-sql sel)
        expect "SELECT * FROM tbl_person AS tbl_person0 WHERE tbl_person0.id > 10"] 
    (is (= expect actual))
    ))

(deftest to-sql-select-select
  (let [
        p (->Base :tbl_person)
        sel (->Select (->Select p `(:> ~(->Col p "id") 10)) `(:< ~(->Col p "id") 30))
        actual (to-sql sel)
        expect "SELECT * FROM (SELECT * FROM tbl_person AS tbl_person0 WHERE tbl_person0.id > 10) AS s0 WHERE tbl_person0.id < 30"] 
    (is (= expect actual))
    ))

(deftest to-sql-select-and
  (let [
        p (->Base :tbl_person)
        sel (->Select p `(:and (:> ~(->Col p "id") 10) (:< ~(->Col p "id") 30)))
        actual (to-sql sel)
        expect "SELECT * FROM tbl_person AS tbl_person0 WHERE (tbl_person0.id > 10) and (tbl_person0.id < 30)"] 
    (is (= expect actual))
    ))

(deftest to-sql-join
  (let [
        p (->Base :tbl_person)
        c (->Base :tbl_city)
        sel (->Join p c {(->Col p "city") (->Col c "city_code")})
        actual (to-sql sel)
        expect "SELECT * FROM tbl_person AS tbl_person0 JOIN tbl_city AS tbl_city0 ON tbl_person0.city = tbl_city0.city_code"]
    (is (= expect actual))
    ))

(deftest to-sql-theta-join
  (let [
        p (->Base :tbl_person)
        c (->Base :tbl_city)
        sel (->ThetaJoin p c {(->Col p "city") (->Col c "city_code")} `(:> ~(->Col p "id") 2))
        actual (to-sql sel)
        expect "SELECT * FROM tbl_person AS tbl_person0 JOIN tbl_city AS tbl_city0 ON tbl_person0.city = tbl_city0.city_code WHERE tbl_person0.id > 2"] 
    (is (= expect actual))
    ))

(deftest to-sql-vector-aggregate
  (let [
        p (->Base :tbl_person)
        aggr (->Aggregate p [(->Col p "city")] `(:avg ~(->Col p "age")) '())
        actual (to-sql aggr)
        expect "SELECT tbl_person0.city, avg(tbl_person0.age) FROM tbl_person AS tbl_person0 GROUP BY tbl_person0.city"] 
    (is (= expect actual))
    ))

(deftest to-sql-scalar-aggregate
  (let [
        p (->Base :tbl_person)
        aggr (->Aggregate person [] `(:avg ~(->Col p "age")) `(:> ~(->Col p "id") 2))
        actual (to-sql aggr)
        expect "SELECT avg(tbl_person0.age) FROM tbl_person AS tbl_person0 WHERE tbl_person0.id > 2"] 
    (is (= expect actual))
    ))

(deftest query-sql-base
  (let [
        p (->Base :tbl_person)
        actual (query-sql p data)
        expect '(
                 {"id" 1, "name" "alex", "city" "SH", "age" 36} 
                 {"id" 2, "name" "alexon", "city" "BJ", "age" 30} 
                 {"id" 3, "name" "richard", "city" "SH", "age" 28})]
    (is (= expect actual))
    ))

(deftest query-sql-project
  (let [
        p (->Base :tbl_person)
        proj (->Project p `(~(->Col p "id")))
        actual (query-sql proj data)
        expect [{"tbl_person0.id" 1} {"tbl_person0.id" 2} {"tbl_person0.id" 3}]] 
    (is (= expect actual))
    ))

(deftest query-sql-select
  (let [
        p (->Base :tbl_person)
        sel (->Select p `(:> ~(->Col p "id") 2))
        actual (query-sql sel data)
        expect [{"tbl_person0.id" 3, "tbl_person0.name" "richard", "tbl_person0.city" "SH", "tbl_person0.age" 28}]] 
    (is (= expect actual))
    ))

(deftest query-sql-select-select
  (let [
        p (->Base :tbl_person)
        sub-p (->Select p `(:> ~(->Col p "id") 1))
        sel (->Select sub-p `(:< ~(->Col sub-p "id") 3))
        actual (query-sql sel data)
        expect [{"s0.id" 2, "s0.name" "alexon", "s0.city" "BJ", "s0.age" 30}]] 
    (is (= expect actual))
    ))

(deftest query-sql-join
  (let [
        sel (->Join person city {"city" "city_code"})
        actual (query-sql sel data)
        expect [
                {"city_code" "SH", "city_name" "ShangHai", "id" 1, "name" "alex", "city" "SH" "age" 36} 
                {"city_code" "BJ", "city_name" "BeiJing", "id" 2, "name" "alexon", "city" "BJ" "age" 30} 
                {"city_code" "SH", "city_name" "ShangHai", "id" 3, "name" "richard", "city" "SH" "age" 28}
                ]] 
    (is (= expect actual))
    ))

(deftest query-sql-theta-join
  (let [
        sel (->ThetaJoin person city {"city" "city_code"} '(:> "id" 2))
        actual (query-sql sel data)
        expect [
                {"city_code" "SH", "city_name" "ShangHai", "id" 3, "name" "richard", "city" "SH" "age" 28}
                ]] 
    (is (= expect actual))
    ))

(deftest query-sql-aggregate
  (let [
        aggr (->Aggregate person ["city"] '(:avg "age") '())
        actual (query-sql aggr data)
        aggr-col-name (keyword "avg(age)")
        expect [{"city" "SH", aggr-col-name 32} {"city" "BJ", aggr-col-name 30}]
        ]
    (is (= expect actual))
    ))

(deftest query-sql-apply-whose-relation-and-expr-has-no-relation
  (let [
        aggr (->Aggregate person [] '(:avg "age") '(:> "id" 2))
        appl (->Apply city aggr)
        actual (query-sql appl data)
        aggr-col-name (keyword "avg(age)")
        expect [{aggr-col-name 94/3, "city_code" "SH", "city_name" "ShangHai"} {aggr-col-name 94/3, "city_code" "BJ", "city_name" "BeiJing"}]
        ]
    (is (= expect actual))
    ))
