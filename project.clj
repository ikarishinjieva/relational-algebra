(defproject relational_algebra "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"] 
                 [org.clojure/tools.logging "0.4.0"]
                 [log4j/log4j "1.2.16" :exclusions [javax.mail/mail
                                                  javax.jms/jms
                                                  com.sun.jdmk/jmxtools
                                                  com.sun.jmx/jmxri]]
                 [aprint "0.1.3"]
                 [es.usc.citius.hipster/hipster-all "1.0.1"]]
  :main ^:skip-aot relational-algebra.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}}
  :mirrors {"central" {:name "central"
                       :url "http://maven.aliyun.com/nexus/content/groups/public/"}
            "sonatype-oss-public" {
                                   :name "sonatype-oss-public"
                                   :url "https://oss.sonatype.org/content/groups/public/"
                                   }}
  )
