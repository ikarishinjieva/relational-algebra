(ns relational-algebra.convert-test
  (:require [clojure.test :refer :all]
            [relational-algebra.core :refer :all]
            [relational-algebra.convert :refer :all]
            [clojure.tools.logging :as log]))

(def data {
           "tbl_person" [
                        {"id" 1, "name" "alex", "city" "SH", "age" 36}
                        {"id" 2, "name" "alexon", "city" "BJ", "age" 30}
                        {"id" 3, "name" "richard", "city" "SH", "age" 28}
                        ]
           "tbl_city" [
                      {"city_code" "SH", "city_name" "ShangHai"}
                      {"city_code" "BJ", "city_name" "BeiJing"}
                      ]
           "tbl_position" [
                      {"city_code" "SH", "position" "South"}
                      {"city_code" "BJ", "position" "North"}
                      ]
           })

(def metas {
           "tbl_person" ["id", "name", "city", "age"],
           "tbl_city" ["city_code", "city_name"],
           "tbl_position" ["city_code", "position"]
           })

(deftest test-sql-convert-select-commutative
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        inner-select (->Select p `(:> ~(->Col p "id") 1))
        raw (->Select inner-select `(:< ~(->Col inner-select "id") 3))
        actual (to-sql (convert-select-commutative raw))
        expect "SELECT * FROM (SELECT * FROM tbl_person AS tbl_person0 WHERE tbl_person0.id < 3) AS s0 WHERE s0.id > 1"]
    (is (= expect actual))
    ))

(deftest test-data-convert-select-commutative
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        inner-select (->Select p `(:> ~(->Col p "id") 1))
        raw (->Select inner-select `(:< ~(->Col inner-select "id") 3))
        expect (remove-data-table-prefix (query-sql raw data))
        actual (remove-data-table-prefix (query-sql (convert-select-commutative raw) data))
        ]
    (is (= expect actual))
    ))

(deftest test-sql-convert-join-associative
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        c (->Base "tbl_city" (get metas "tbl_city"))
        pos (->Base "tbl_position" (get metas "tbl_position"))
        inner-join (->Join p c {(->Col p "city") (->Col c "city_code")})
        raw (->Join inner-join pos {(->Col inner-join "city_code") (->Col pos "city_code")})
        actual (to-sql (convert-join-associative raw))
        expect "SELECT * FROM tbl_person AS tbl_person0 JOIN (SELECT * FROM tbl_city AS tbl_city0 JOIN tbl_position AS tbl_position0 ON tbl_city0.city_code = tbl_position0.city_code) AS j0 ON tbl_person0.city = j0.city_code"] 
    (is (= expect actual))
    ))

(deftest test-data-convert-join-associative
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        c (->Base "tbl_city" (get metas "tbl_city"))
        pos (->Base "tbl_position" (get metas "tbl_position"))
        inner-join (->Join p c {(->Col p "city") (->Col c "city_code")})
        raw (->Join inner-join pos {(->Col inner-join "city_code") (->Col pos "city_code")})
        expect (remove-data-table-prefix (query-sql raw data))
        actual (remove-data-table-prefix (query-sql (convert-join-associative raw) data))
        ]
    (is (= expect actual))
    ))

(deftest test-sql-convert-select_join-to-theta_join
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        c (->Base "tbl_city" (get metas "tbl_city"))
        inner (->Join p c {(->Col p "city") (->Col c "city_code")})
        raw (->Select inner `(:> ~(->Col inner "id") 2))
        actual (to-sql (convert-select_join-to-theta_join raw))
        expect "SELECT * FROM tbl_person AS tbl_person0 JOIN tbl_city AS tbl_city0 ON tbl_person0.city = tbl_city0.city_code WHERE j0.id > 2"] 
    (is (= expect actual))
    ))

(deftest test-data-convert-select_join-to-theta_join
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        c (->Base "tbl_city" (get metas "tbl_city"))
        inner (->Join p c {(->Col p "city") (->Col c "city_code")})
        raw (->Select inner `(:> ~(->Col inner "id") 2))
        expect (remove-data-table-prefix (query-sql raw data))
        actual (remove-data-table-prefix (query-sql (convert-select_join-to-theta_join raw) data))
        ] 
    (is (= expect actual))
    ))

(deftest test-sql-convert-select_theta_join-to-theta_join
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        c (->Base "tbl_city" (get metas "tbl_city"))
        inner (->ThetaJoin p c {(->Col p "city") (->Col c "city_code")} `(:> ~(->Col p "id") 2))
        raw (->Select inner `(:< ~(->Col inner "id") 10))
        actual (to-sql (convert-select_theta_join-to-theta_join raw))
        expect "SELECT * FROM tbl_person AS tbl_person0 JOIN tbl_city AS tbl_city0 ON tbl_person0.city = tbl_city0.city_code WHERE (tj0.id < 10) and (tbl_person0.id > 2)"] 
    (is (= expect actual))
    ))

(deftest test-data-convert-select_theta_join-to-theta_join
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        c (->Base "tbl_city" (get metas "tbl_city"))
        inner (->ThetaJoin p c {(->Col p "city") (->Col c "city_code")} `(:> ~(->Col p "id") 2))
        raw (->Select inner `(:< ~(->Col inner "id") 10))
        expect (remove-data-table-prefix (query-sql raw data))
        actual (remove-data-table-prefix (query-sql (convert-select_theta_join-to-theta_join raw) data))
        ]
    (is (= expect actual))
    ))

(deftest test-sql-convert-apply_whose_expr_not_resolved_from_relation-to-join
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        c (->Base "tbl_city" (get metas "tbl_city"))
        aggr (->Aggregate p [] `(:avg ~(->Col p "age")) `(:> ~(->Col p "id") 2))
        appl (->Apply c aggr)
        actual (to-sql (convert-apply_whose_expr_not_resolved_from_relation-to-join appl))
        expect "SELECT * FROM tbl_city AS tbl_city0 JOIN (SELECT avg(tbl_person0.age) FROM tbl_person AS tbl_person0 WHERE tbl_person0.id > 2) AS a0"] 
    (is (= expect actual))
    ))

(deftest test-data-convert-apply_whose_expr_not_resolved_from_relation-to-join
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        c (->Base "tbl_city" (get metas "tbl_city"))
        aggr (->Aggregate p [] `(:avg ~(->Col p "age")) `(:> ~(->Col p "id") 2))
        appl (->Apply c aggr)
        expect (remove-data-table-prefix (query-sql appl data))
        actual (remove-data-table-prefix (query-sql (convert-apply_whose_expr_not_resolved_from_relation-to-join appl) data))]
    (is (= expect actual))
    ))

;TODO test-sql-convert-apply_whose_select_expr_not_resolved_from_relation-to-theta_join

(deftest test-data-convert-apply_whose_select_expr_not_resolved_from_relation-to-theta_join
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        c (->Base "tbl_city" (get metas "tbl_city"))
        expr (->Select p `(:> ~(->Col p "id") 2))
        appl (->Apply c expr)
        expect (remove-data-table-prefix (query-sql appl data))
        actual (remove-data-table-prefix (query-sql (convert-apply_whose_select_expr_not_resolved_from_relation-to-theta_join appl) data))]
    (is (= expect actual))
    ))

;TODO test-sql-convert-apply_select-to-select_apply

(deftest test-data-convert-apply_select-to-select_apply
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        c (->Base "tbl_city" (get metas "tbl_city"))
        expr (->Select p `(:= ~(->Col p "city") ~(->Col c "city_code")))
        appl (->Apply c expr)
        expect (remove-data-table-prefix (query-sql appl data))
        actual (remove-data-table-prefix (query-sql (convert-apply_select-to-select_apply appl) data))]
    (is (= expect actual))
    ))

;TODO test-sql-convert-apply_project-to-project_apply

(deftest test-data-convert-apply_project-to-project_apply
  (let [
        p (->Base "tbl_person" (get metas "tbl_person"))
        c (->Base "tbl_city" (get metas "tbl_city"))
        expr (->Project p `(~(->Col p "id") ~(->Col p "name")))
        appl (->Apply c expr)
        expect (remove-data-table-prefix (query-sql appl data))
        actual (remove-data-table-prefix (query-sql (convert-apply_project-to-project_apply appl) data))]
    (is (= expect actual))
    ))