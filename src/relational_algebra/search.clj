(ns relational-algebra.search
  (:require [relational-algebra.core :refer :all]
            [relational-algebra.convert :refer :all]
            [aprint.core :refer :all])
  (:import 
    (es.usc.citius.hipster.algorithm Hipster)
    (es.usc.citius.hipster.model.problem ProblemBuilder)
    (es.usc.citius.hipster.model.function TransitionFunction)
    (es.usc.citius.hipster.model.function CostFunction)
    (es.usc.citius.hipster.model.function HeuristicFunction)
    (es.usc.citius.hipster.model.function.impl StateTransitionFunction)
    (es.usc.citius.hipster.util Predicate)))

(def state-transition-fn
  (proxy [StateTransitionFunction TransitionFunction] [] 
    (successorsOf [stat] 
                  (let [
                        converts [
									convert-select-commutative
									convert-base-to-select
									convert-join-associative
									convert-select_join-to-theta_join
									convert-select_theta_join-to-theta_join
									convert-apply_whose_expr_not_resolved_from_relation-to-join
									convert-apply_whose_select_expr_not_resolved_from_relation-to-theta_join
									convert-apply_select-to-select_apply
									convert-apply_project-to-project_apply
									convert-apply_scalar_aggregate-to-vector_aggregate_apply
                                  ]
                        try-convert (fn [tbl convert] (try 
                                      (convert tbl)
                                      (catch java.lang.AssertionError e (identity nil))))
                        reduce-fn (fn [res convert]
                                    (let [
                                      	replacements (iterate-tbl stat 
	                                                 (fn [tbl] 
	                                                   (if-not (nil? (try-convert tbl convert))
	                                                     (identity {tbl (convert tbl)})
	                                                     nil)))
                                        replacements (remove nil? replacements)
                                      ]
                                      (concat res (map #(replace-tbl stat %1) replacements))))
                        res (reduce reduce-fn [] converts)
                        ]
              		(if (empty? res) [stat] res)))))

(def cost-fn
  (proxy [CostFunction] [] 
    (evaluate [transition] 
              (double 0))))

(def heuristic-fn
  (proxy [HeuristicFunction] [] 
    (estimate [state] 
              (double (estimate-cost state)))))

(def predicate-fn
  (proxy [Predicate] []
    (apply [node] (identity false))))

(defn search [init-status]
  (let [
        p (ProblemBuilder/create)
        p (.initialState p init-status)
        p (.defineProblemWithoutActions p)
        p (.useTransitionFunction p state-transition-fn)
        p (.useCostFunction p cost-fn)
        p (.useHeuristicFunction p heuristic-fn)
        p (.build p)
        p (Hipster/createAnnealingSearch p nil nil nil nil)
        res (.search p predicate-fn)
        goal (.state (first (.getGoalNodes res)))
        ]
    (identity goal)
    ; (print "GOAL:" (map #(aprint (.state %1)) (.getGoalNodes res)) "\n")
    ; (print "PATH:" (map #(do (aprint %1) (print "\n")) (.getOptimalPaths res)) "\n"))
  ))