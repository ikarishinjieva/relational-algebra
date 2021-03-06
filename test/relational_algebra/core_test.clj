(ns relational-algebra.core-test
  (:require [clojure.test :refer :all]
            [relational-algebra.core :refer :all]))

(def data {
           "tbl_person" [
                        {"id" 1, "name" "alex", "city" "SH", "age" 36}
                        {"id" 2, "name" "alexon", "city" "BJ", "age" 30}
                        {"id" 3, "name" "richard", "city" "SH", "age" 28}
                        ]
           "tbl_city" [
                      {"city_code" "SH", "city_name" "ShangHai"}
                      {"city_code" "BJ", "city_name" "BeiJing"}
                      {"city_code" "SZ", "city_name" "ShenZhen"}
                      ]
           })

(def metas {
           "tbl_person" ["id", "name", "city", "age"],
           "tbl_city" ["city_code", "city_name"],
           })

(deftest to-sql-base
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        actual (to-sql p)
        expect "tbl_person"] 
    (is (= expect actual))
    ))

(deftest to-sql-project
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        proj (->Project p `(~(->Col p "id") ~(->Col p "name")))
        actual (to-sql proj)
        expect "SELECT tbl_person0.id, tbl_person0.name FROM tbl_person AS tbl_person0"] 
    (is (= expect actual))
    ))

(deftest to-sql-select
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        sel (->Select p `(:> ~(->Col p "id") 10))
        actual (to-sql sel)
        expect "SELECT * FROM tbl_person AS tbl_person0 WHERE tbl_person0.id > 10"] 
    (is (= expect actual))
    ))

(deftest to-sql-select-select
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        sel (->Select (->Select p `(:> ~(->Col p "id") 10)) `(:< ~(->Col p "id") 30))
        actual (to-sql sel)
        expect "SELECT * FROM (SELECT * FROM tbl_person AS tbl_person0 WHERE tbl_person0.id > 10) AS s0 WHERE tbl_person0.id < 30"] 
    (is (= expect actual))
    ))

(deftest to-sql-select-and
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        sel (->Select p `(:and (:> ~(->Col p "id") 10) (:< ~(->Col p "id") 30)))
        actual (to-sql sel)
        expect "SELECT * FROM tbl_person AS tbl_person0 WHERE (tbl_person0.id > 10) and (tbl_person0.id < 30)"] 
    (is (= expect actual))
    ))

(deftest to-sql-join
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        c (->Base "tbl_city" (get metas "tbl_city"))
        sel (->Join p c {(->Col p "city") (->Col c "city_code")} [] :inner)
        actual (to-sql sel)
        expect "SELECT * FROM tbl_person AS tbl_person0 JOIN tbl_city AS tbl_city0 ON tbl_person0.city = tbl_city0.city_code"]
    (is (= expect actual))
    ))

(deftest to-sql-left-join
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        c (->Base "tbl_city" (get metas "tbl_city"))
        sel (->Join p c {(->Col p "city") (->Col c "city_code")} [] :left)
        actual (to-sql sel)
        expect "SELECT * FROM tbl_person AS tbl_person0 LEFT JOIN tbl_city AS tbl_city0 ON tbl_person0.city = tbl_city0.city_code"]
    (is (= expect actual))
    ))

(deftest to-sql-theta-join
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        c (->Base "tbl_city" (get metas "tbl_city"))
        sel (->Join p c {(->Col p "city") (->Col c "city_code")} `(:> ~(->Col p "id") 2) :inner)
        actual (to-sql sel)
        expect "SELECT * FROM tbl_person AS tbl_person0 JOIN tbl_city AS tbl_city0 ON tbl_person0.city = tbl_city0.city_code WHERE tbl_person0.id > 2"] 
    (is (= expect actual))
    ))

(deftest to-sql-vector-aggregate
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        aggr (->Aggregate p [(->Col p "city")] `(:avg ~(->Col p "age")) '())
        actual (to-sql aggr)
        expect "SELECT tbl_person0.city, avg(tbl_person0.age) FROM tbl_person AS tbl_person0 GROUP BY tbl_person0.city"] 
    (is (= expect actual))
    ))

(deftest to-sql-scalar-aggregate
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        aggr (->Aggregate p [] `(:avg ~(->Col p "age")) `(:> ~(->Col p "id") 2))
        actual (to-sql aggr)
        expect "SELECT avg(tbl_person0.age) FROM tbl_person AS tbl_person0 WHERE tbl_person0.id > 2"] 
    (is (= expect actual))
    ))

(deftest to-sql-apply-whose-relation-and-expr-has-no-relation
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        c (->Base "tbl_city" (get metas "tbl_city"))
        aggr (->Aggregate p [] `(:avg ~(->Col p "age")) `(:> ~(->Col p "id") 2))
        appl (make-Apply :relation c, :expr aggr, :condition `(:> ~aggr 10))
        actual (to-sql appl)
        expect "SELECT * from tbl_city AS tbl_city0 WHERE (SELECT avg(tbl_person0.age) FROM tbl_person AS tbl_person0 WHERE tbl_person0.id > 2) > 10"
        ]
    (is (= expect actual))
    ))

(deftest to-sql-apply-whose-relation-and-expr-has-relation
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        c (->Base "tbl_city" (get metas "tbl_city"))
        aggr (->Aggregate p [] `(:avg ~(->Col p "age")) `(:= ~(->Col p "city") ~(->Col c "city_code")))
        appl (make-Apply :relation c, :expr aggr, :condition `(:> ~aggr 10))
        actual (to-sql appl)
        expect "SELECT * from tbl_city AS tbl_city0 WHERE (SELECT avg(tbl_person0.age) FROM tbl_person AS tbl_person0 WHERE tbl_person0.city = tbl_city0.city_code) > 10"
        ]
    (is (= expect actual))
    ))

(deftest query-sql-base
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        actual (query-sql p data)
        expect '(
                 {"id" 1, "name" "alex", "city" "SH", "age" 36} 
                 {"id" 2, "name" "alexon", "city" "BJ", "age" 30} 
                 {"id" 3, "name" "richard", "city" "SH", "age" 28})]
    (is (= expect actual))
    ))

(deftest query-sql-project
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        proj (->Project p `(~(->Col p "id")))
        actual (query-sql proj data)
        expect [{"tbl_person0.id" 1} {"tbl_person0.id" 2} {"tbl_person0.id" 3}]] 
    (is (= expect actual))
    ))

(deftest query-sql-select
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        sel (->Select p `(:> ~(->Col p "id") 2))
        actual (query-sql sel data)
        expect [{"tbl_person0.id" 3, "tbl_person0.name" "richard", "tbl_person0.city" "SH", "tbl_person0.age" 28}]] 
    (is (= expect actual))
    ))

(deftest query-sql-select-select
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        sub-p (->Select p `(:> ~(->Col p "id") 1))
        sel (->Select sub-p `(:< ~(->Col sub-p "id") 3))
        actual (query-sql sel data)
        expect [{"s0.id" 2, "s0.name" "alexon", "s0.city" "BJ", "s0.age" 30}]] 
    (is (= expect actual))
    ))

(deftest query-sql-join
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        c (->Base "tbl_city" (get metas "tbl_city"))
        sel (->Join p c {(->Col p "city") (->Col c "city_code")} [] :inner)
        actual (query-sql sel data)
        expect [
                {"tbl_city0.city_code" "SH", "tbl_city0.city_name" "ShangHai", "tbl_person0.id" 1, "tbl_person0.name" "alex", "tbl_person0.city" "SH", "tbl_person0.age" 36} 
                {"tbl_city0.city_code" "BJ", "tbl_city0.city_name" "BeiJing", "tbl_person0.id" 2, "tbl_person0.name" "alexon", "tbl_person0.city" "BJ", "tbl_person0.age" 30} 
                {"tbl_city0.city_code" "SH", "tbl_city0.city_name" "ShangHai", "tbl_person0.id" 3, "tbl_person0.name" "richard", "tbl_person0.city" "SH", "tbl_person0.age" 28}
                ]] 
    (is (= expect actual))
    ))

(deftest query-sql-left-join
  (let [
        c (->Base "tbl_city" (get metas "tbl_city"))
        p (->Base "tbl_person" (get metas "tbl_person"))
        sel (->Join c p {(->Col c "city_code") (->Col p "city")} [] :left)
        actual (query-sql sel data)
        expect [{"tbl_person0.id" 1, "tbl_person0.name" "alex", "tbl_person0.city" "SH", "tbl_person0.age" 36, "tbl_city0.city_code" "SH", "tbl_city0.city_name" "ShangHai"}
                {"tbl_person0.id" 3, "tbl_person0.name" "richard", "tbl_person0.city" "SH", "tbl_person0.age" 28, "tbl_city0.city_code" "SH", "tbl_city0.city_name" "ShangHai"}
                {"tbl_person0.id" 2, "tbl_person0.name" "alexon", "tbl_person0.city" "BJ", "tbl_person0.age" 30, "tbl_city0.city_code" "BJ", "tbl_city0.city_name" "BeiJing"}
                {"tbl_city0.city_code" "SZ", "tbl_city0.city_name" "ShenZhen"}
                ]] 
    (is (= expect actual))
    ))

(deftest query-sql-theta-join
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        c (->Base "tbl_city" (get metas "tbl_city"))
        sel (->Join p c {(->Col p "city") (->Col c "city_code")} `(:> ~(->Col p "id") 2) :inner)
        actual (query-sql sel data)
        expect [
                {"tbl_city0.city_code" "SH", "tbl_city0.city_name" "ShangHai", "tbl_person0.id" 3, "tbl_person0.name" "richard", "tbl_person0.city" "SH", "tbl_person0.age" 28}
                ]] 
    (is (= expect actual))
    ))

(deftest query-sql-aggregate
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        aggr (->Aggregate p [(->Col p "city")] `(:avg ~(->Col p "age")) '())
        actual (query-sql aggr data)
        expect [{"tbl_person0.city" "SH", "avg(tbl_person0.age)" 32} {"tbl_person0.city" "BJ", "avg(tbl_person0.age)" 30}]
        ]
    (is (= expect actual))
    ))

(deftest query-sql-aggregate-with-condition
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        aggr (->Aggregate p [(->Col p "city")] `(:avg ~(->Col p "age")) `(:= ~(->Col p "city") "SH"))
        actual (query-sql aggr data)
        expect [{"tbl_person0.city" "SH", "avg(tbl_person0.age)" 32}]
        ]
    (is (= expect actual))
    ))

(deftest query-sql-aggregate-scalar-on-empty-tbl
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        aggr (->Aggregate p [] `(:avg ~(->Col p "age")) `(:= ~(->Col p "city") "NOT_EXIST"))
        actual (query-sql aggr data)
        expect [{"avg(tbl_person0.age)" 0}]
        ]
    (is (= expect actual))
    ))

(deftest query-sql-apply-whose-relation-and-expr-has-no-relation-without-condition
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        c (->Base "tbl_city" (get metas "tbl_city"))
        aggr (->Aggregate p [] `(:avg ~(->Col p "age")) `(:> ~(->Col p "id") 2))
        appl (make-Apply :relation c, :expr aggr)
        actual (query-sql appl data)
        expect [
                {"avg(j0.age)" 28, "j0.city_code" "SH", "j0.city_name" "ShangHai"} 
                {"avg(j0.age)" 28, "j0.city_code" "BJ", "j0.city_name" "BeiJing"}
                {"avg(j0.age)" 28, "j0.city_code" "SZ", "j0.city_name" "ShenZhen"}]
        ]
    (is (= expect actual))
    ))

(deftest query-sql-apply-whose-relation-and-expr-has-relation-without-condition
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        c (->Base "tbl_city" (get metas "tbl_city"))
        aggr (->Aggregate p [] `(:avg ~(->Col p "age")) `(:= ~(->Col p "city") ~(->Col c "city_code")))
        appl (make-Apply :relation c, :expr aggr)
        actual (query-sql appl data)
        expect [{"avg(j0.age)" 32, "j0.city_code" "SH", "j0.city_name" "ShangHai"} 
                {"avg(j0.age)" 30, "j0.city_code" "BJ", "j0.city_name" "BeiJing"} 
                {"avg(j0.age)" 0, "j0.city_code" "SZ", "j0.city_name" "ShenZhen"}]
        ]
    (is (= expect actual))
    ))
(deftest query-sql-apply-whose-relation-and-expr-has-no-relation-with-condition
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        c (->Base "tbl_city" (get metas "tbl_city"))
        aggr (->Aggregate p [] `(:avg ~(->Col p "age")) `(:> ~(->Col p "id") 2))
        appl (make-Apply :relation c, :expr aggr, :condition `(:> ~aggr 28))
        actual (query-sql appl data)
        expect []
        ]
    (is (= expect actual))
    ))

(deftest query-sql-apply-whose-relation-and-expr-has-relation-without-condition
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        c (->Base "tbl_city" (get metas "tbl_city"))
        aggr (->Aggregate p [] `(:avg ~(->Col p "age")) `(:= ~(->Col p "city") ~(->Col c "city_code")))
        appl (make-Apply :relation c, :expr aggr, :condition `(:> ~aggr 28))
        actual (query-sql appl data)
        expect [{"avg(j0.age)" 32, "j0.city_code" "SH", "j0.city_name" "ShangHai"} 
                {"avg(j0.age)" 30, "j0.city_code" "BJ", "j0.city_name" "BeiJing"} ]
        ]
    (is (= expect actual))
    ))