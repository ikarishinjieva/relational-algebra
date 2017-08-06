(ns relational-algebra.convert-test
  (:require [clojure.test :refer :all]
            [relational-algebra.core :refer :all]
            [relational-algebra.convert :refer :all]))

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

(deftest test-convert-select-cascade
  (let [
        sel (->Select (->Select person '(:> :id 1)) '(:< :id 3))
        actual (to-sql (convert-select-cascade sel))
        expect "SELECT * FROM (SELECT * FROM tbl_person WHERE id < 3) WHERE id > 1"] 
    (is (= expect actual))
    ))
