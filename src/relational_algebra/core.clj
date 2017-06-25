(ns relational-algebra.core
    (:gen-class))

(def employees '({:id 1 :salary 100}, {:id 2 :salary 200}))

(defn project [table, cols]
      (map #(select-keys % cols) table))

(defn -main
      [& args]
      (println employees)
      (println (project employees #{:salary})))