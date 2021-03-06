(ns probe.ns
  (:require [probe.core :as p]
            [probe.wrap :as wrap]))

;; ============================================================================
;; Utils

(defn- make-symbol
  [ns sym]
  (symbol (name ns) (name sym)))

(defn- ns-privates
  [ns]
  (into {}
        (clojure.set/difference (-> ns ns-interns set)
                                (-> ns ns-publics set))))

(defn- probe-var-fns*
  ([f vars ns]
   (doall
    (map (fn [v]
           (let [s (make-symbol ns v)]
             (when (-> (wrap/as-var s) var-get fn?)
               (f s))))
         vars))))

;; ============================================================================
;; Namespace Probe API

(defn probe-ns!
  ([ns]
   (probe-var-fns* p/probe-fn! (keys (ns-publics ns)) ns))
  ([tags ns]
   (probe-var-fns* (partial p/probe-fn! tags) (keys (ns-publics ns)) ns)))

(defn unprobe-ns!
  [ns]
  (probe-var-fns* p/unprobe-fn! (keys (ns-publics ns)) ns))

(defn probe-ns-private!
  ([ns]
   (probe-var-fns* p/probe-fn! (keys (ns-privates ns)) ns))
  ([tags ns]
   (probe-var-fns* (partial p/probe-fn! tags) (keys (ns-privates ns)) ns)))

(defn unprobe-ns-private!
  [ns]
  (probe-var-fns* p/unprobe-fn! (keys (ns-privates ns)) ns))

(defn probe-ns-all!
  ([ns]
   (probe-var-fns* p/probe-fn! (keys (ns-interns ns)) ns))
  ([tags ns]
   (probe-var-fns* (partial p/probe-fn! tags) (keys (ns-interns ns)) ns)))

(defn unprobe-ns-all!
  [ns]
  (probe-var-fns* p/unprobe-fn! (keys (ns-interns ns)) ns))

(defn- make-ns-pfns!
  [sinks [subscribe ns-sym tags
          {:keys [public private level transforms]
           :or   {public true, private false, level 0,
                  transforms [identity]}}]]
  (assert (not (every? false? [public private]))
          (format "At least one of :public or :private must be true in definition for %s."
                  subscribe))
  (let [[pfn upfn] (cond
                    (and public private) [probe-ns-all! unprobe-ns-all!]
                    private              [probe-ns-private! unprobe-ns-private!]
                    public               [probe-ns! unprobe-ns!])]
    {:probe   `(do
                 (require '~ns-sym) ;; ns must be loaded to probe it
                 (~pfn ~(conj tags subscribe) '~ns-sym)
                 (doseq [s# ~sinks]
                   (p/subscribe #{~subscribe} s#
                                :transform #(-> % ~@transforms))))
     :unprobe `(do
                 (~upfn '~ns-sym)
                 (doseq [s# ~sinks]
                   (p/unsubscribe #{~subscribe} s#)))
     :subscribe subscribe}))

(defmacro defprobes
  [name* sinks & probe-defs]
  (let [pfns# (map (partial make-ns-pfns! sinks) probe-defs)]
    (assert (apply distinct? (map first probe-defs))
            "All definitions must define distinct subscriptions.")
    `(do
       (defn ~(symbol (str "install-" (name name*) "!")) []
         ~@(map :probe pfns#))
       (defn ~(symbol (str "uninstall-" (name name*) "!")) []
         ~@(map :unprobe pfns#))
       ~@(for [m# pfns#]
           `(do
              (defn ~(symbol (str "probe-" (-> m# :subscribe name) "!")) []
                ~(:probe m#))
              (defn ~(symbol (str "unprobe-" (-> m# :subscribe name) "!")) []
                ~(:unprobe m#)))))))
