(ns relational-algebra.search-test
  (:require [clojure.test :refer :all]
            [relational-algebra.search :refer :all]
            [relational-algebra.core :refer :all]
            [relational-algebra.convert :refer :all]
            [aprint.core :refer :all]))

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
           "tbl_tpch_customer" [
                                {"custkey_c" "001"}
                                {"custkey_c" "002"}
                                ]
           "tbl_tpch_order" [
                             {"custkey_o" "001", "price" 102}
                             {"custkey_o" "001", "price" 421}
                             {"custkey_o" "002", "price" 178}
                             {"custkey_o" "002", "price" 631}
                             {"custkey_o" "004", "price" 555}
                             ]
           })

(def metas {
           "tbl_person" ["id", "name", "city", "age"],
           "tbl_city" ["city_code", "city_name"],
           "tbl_position" ["city_code", "position"]
           "tbl_tpch_customer" ["custkey_c"]
           "tbl_tpch_order" ["custkey_o", "price"]
           })

(deftest test-annealing-search
  "Q1 from <Orthogonal Optimization of Subqueries and Aggregation>:
    SELECT C_CUSTKEY
    FROM CUSTOMER
    WHERE 1000000 <
        (SELECT SUM(O_TOTALPRICE)
        FROM ORDERS
        WHERE O_CUSTKEY = C_CUSTKEY)"
        (let [
              cust (->Base "tbl_tpch_customer" (get metas "tbl_tpch_customer"))
              order (->Base "tbl_tpch_order" (get metas "tbl_tpch_order"))
              order-with-cond (->Select order `(:= ~(->Col order "custkey_o") ~(->Col cust "custkey_c")))
              aggr (->Aggregate order-with-cond [] `(:sum ~(->Col order-with-cond "price")) [])
              appl (make-Apply :relation cust, :expr aggr, :condition [:< 1000000 aggr])
              expect (remove-data-table-prefix (query-sql appl data))
          
              goal (first (search appl))
              goal-rel (.rel goal)
              actual (remove-data-table-prefix (query-sql goal-rel data))
              ]
          (print-path-costs goal)
          (is (= expect actual))))

