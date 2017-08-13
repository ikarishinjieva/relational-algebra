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
        raw (->Select (->Select person '(:> :id 1)) '(:< :id 3))
        actual (to-sql (convert-select-commutative raw))
        expect "SELECT * FROM (SELECT * FROM tbl_person WHERE id < 3) WHERE id > 1"] 
    (is (= expect actual))
    ))

(deftest test-convert-join-associative
  (let [
        raw (->Join (->Join person city {:city :city_code}) position {:city_code :city_code})
        actual (to-sql (convert-join-associative raw))
        expect "SELECT * FROM tbl_person JOIN (SELECT * FROM tbl_city JOIN tbl_position ON city_code = city_code) ON city = city_code"] 
    (is (= expect actual))
    ))

(deftest test-convert-select-join-to-theta-join
  (let [
        raw (->Select (->Join person city {:city :city_code}) '(:> :id 2))
        actual (to-sql (convert-select-join-to-theta-join raw))
        expect "SELECT * FROM tbl_person JOIN tbl_city ON city = city_code WHERE id > 2"] 
    (is (= expect actual))
    ))
