(ns clojurecast.cluster
  (:require [clojurecast.core :as cc]
            [com.stuartsierra.component :as com])
  (:import [com.hazelcast.core Cluster]))

(defn ^long current-time-millis
  []
  (.getClusterTime (cc/cluster)))

(defn ^com.hazelcast.core.Member local-member
  []
  (.getLocalMember (cc/cluster)))

(defn ^java.util.Set members
  []
  (.getMembers (cc/cluster)))

(defn ^String local-member-uuid
  []
  (.getUuid (local-member)))

(defn ^com.hazelcast.core.IMap membership-listeners
  []
  (cc/distributed-map "cluster/membership-listeners"))

(defn add-membership-listener
  [listener & {:keys [id]}]
  (when-not (and id (.containsKey (membership-listeners) id))
    (let [registration-id (.addMembershipListener (cc/cluster) listener)]
      (when id
        (.put (membership-listeners) id registration-id))
      registration-id)))

(defn remove-membership-listener
  [^String id]
  (if-let [registration-id (.get (membership-listeners) id)]
    (do
      (.remove (membership-listeners) id)
      (.removeMembershipListener (cc/cluster) registration-id))
    (.removeMembershipListener (cc/cluster) id)))

(defn is-master?
  []
  (identical? (first (members)) (local-member)))
