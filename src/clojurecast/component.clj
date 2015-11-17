(ns clojurecast.component
  (:require [com.stuartsierra.component :as com]))

(def ^:dynamic *component*)

(defprotocol Component
  (initialized? [component] "Return true if component has been initialized. Component initialization should be thought of as a once-only operation. For example, to start a database component for your application you may need to first install a schema. Because schemas are long lived, you only want to initialize them when you are first configuration your application. Subsequent upgrades to this type of component state should live in migration logic.")
  (started? [component] "Return true if the component is running. The lifecycle methods are idemponent, so a running component will not be started more than once.")
  (migrate? [component] "Return true if the `-migrate` method should be called. This is where stateful upgrades to the component should happen. For example, altering a schema for a database component.")
  (-init [component] "Initialize the component, returning the record, possibly with new or modified values (through assoc).")
  (-start [component] "Start the component, returning the record, possibly with new or modified values (through assoc).")
  (-stop [component] "Stop the component, returning the record, possibly with new or modified values (through assoc).")
  (-migrate [component] "Migrate the component, returning the record, possibly with new or modified values (through assoc)."))

(defn init
  "Initialize the component, returning the record, possibly with new or modified values (through assoc)."
  [component]
  (binding [*component* component]
    (if (initialized? component)
      component
      (-init component))))

(defn start
  "Start the component, returning the record, possibly with new or modified values (through assoc)."
  [component]
  (binding [*component* component]
    (if (started? component)
      component
      (-start component))))

(defn stop
  "Stop the component, returning the record, possibly with new or modified values (through assoc)."
  [component]
  (binding [*component* component]
    (if (started? component)
      (-stop component)
      component)))

(defn migrate
  "Migrate the component, returning the record, possibly with new or modified values (through assoc)."
  [component]
  (binding [*component* component]
    (if (migrate? component)
      (-migrate component)
      component)))

(extend-protocol com/Lifecycle
  clojurecast.component.Component
  (start [this]
    (migrate (start (init this))))
  (stop [this]
    (stop this)))
