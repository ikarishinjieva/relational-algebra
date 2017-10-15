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
                                  convert-apply_whose_select_expr_not_resolved_from_relation-to-theta_join
                                  convert-apply_scalar_aggregate-to-vector_aggregate_apply
                                  ]
                        reduce-fn (fn [res convert] 
                                    (try 
                                      (conj res (convert stat))
                                      (catch java.lang.AssertionError e (identity res))))
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
        ]
    (print "GOAL:" (map #(aprint (.state %1)) (.getGoalNodes res)) "\n")
    (print "PATH:" (map #(do (aprint %1) (print "\n")) (.getOptimalPaths res)) "\n")))