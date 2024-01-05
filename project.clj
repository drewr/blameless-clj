(defproject blameless-clj "0.1.0-SNAPSHOT"
  :description "Interact with the Blameless API"
  :url "http://deploy.equinix.com"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[cheshire/cheshire "5.12.0"]
                 [clj-http/clj-http "3.12.3"]
                 [clojure.java-time/clojure.java-time "1.4.2"]
                 [org.flatland/ordered "1.15.11"]
                 [org.clojure/data.csv "1.0.1"]
                 [slingshot/slingshot "0.12.2"]
                 ]
  :profiles {:dev
             {:dependencies
              [[org.clojure/clojure "1.11.1"]]}}
  :repl-options {:init-ns blameless.core})
