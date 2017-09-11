(ns relational-algebra.convert-test
  (:require [clojure.test :refer :all]
            [relational-algebra.core :refer :all]
            [relational-algebra.convert :refer :all]))

(def person (->Base :tbl_person))

(def city (->Base :tbl_city))

(def position (->Base :tbl_position))

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
           :tbl_position [
                      {:city_code "SH" :position "South"}
                      {:city_code "BJ" :position "North"}
                      ]
           })

(deftest test-convert-select-commutative
  (let [
        raw (->Select (->Select person `(:> ~(->Col person :id) 1)) `(:< ~(->Col person :id) 3))
        actual (to-sql (convert-select-commutative raw))
        expect "SELECT * FROM (SELECT * FROM tbl_person AS tbl_person0 WHERE tbl_person0.id < 3) AS s0 WHERE tbl_person0.id > 1"]
    (is (= expect actual))
    ))

(deftest test-convert-join-associative
  (let [
        inner-join (->Join person city {(->Col person :city) (->Col city :city_code)})
        raw (->Join inner-join position {(->Col inner-join :city_code) (->Col position :city_code)})
        actual (to-sql (convert-join-associative raw))
        expect "SELECT * FROM tbl_person AS tbl_person0 JOIN (SELECT * FROM tbl_city AS tbl_city0 JOIN tbl_position AS tbl_position0 ON tbl_city0.city_code = tbl_position0.city_code) AS j0 ON tbl_person0.city = j0.city_code"] 
    (is (= expect actual))
    ))

(deftest test-convert-select-join-to-theta-join
  (let [
        raw (->Select (->Join person city {:city :city_code}) '(:> :id 2))
        actual (to-sql (convert-select-join-to-theta-join raw))
        expect "SELECT * FROM tbl_person AS tbl_person0 JOIN tbl_city AS tbl_city0 ON city = city_code WHERE id > 2"] 
    (is (= expect actual))
    ))

(deftest test-convert-select-theta-join-to-theta-join
  (let [
        raw (->Select (->ThetaJoin person city {:city :city_code} '(:> :id 2)) '(:< :id 10))
        actual (to-sql (convert-select-theta-join-to-theta-join raw))
        expect "SELECT * FROM tbl_person AS tbl_person0 JOIN tbl_city AS tbl_city0 ON city = city_code WHERE (id < 10) and (id > 2)"] 
    (is (= expect actual))
    ))
