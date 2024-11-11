/*
 * Copyright 2023 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.grpc.util;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Maps;
import io.grpc.Attributes;
import io.grpc.ConnectivityState;
import io.grpc.EquivalentAddressGroup;
import io.grpc.Internal;
import io.grpc.LoadBalancer;
import io.grpc.LoadBalancer.ResolvedAddresses;
import io.grpc.LoadBalancer.SubchannelPicker;
import io.grpc.Status;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A base load balancing policy for those policies which has multiple children such as
 * ClusterManager or the petiole policies.  For internal use only.
 */
@Internal
public final class LoadBalancerCollection<K, V> {
  private static final Logger logger = Logger.getLogger(MultiChildLoadBalancer.class.getName());
  private static final int CONNECTIVITY_STATE_COUNT = ConnectivityState.values().length;

  private final ChildCreator<K,V> creator;
  // Modify by replacing the list to release memory when no longer used.
  private List<ChildLbState<K,V>> childLbStates = new ArrayList<>(0);
  // Set to true if currently in the process of handling resolved addresses.
  private boolean resolvingAddresses;
  private final int[] connectivityCounts = new int[CONNECTIVITY_STATE_COUNT];

  interface ChildCreator<K,V> {
    LoadBalancer createChildLb(ChildLbState<K,V> state);

    default void childLbDeleted(ChildLbState<K,V> state) {}
  }

  public LoadBalancerCollection(ChildCreator<K,V> creator) {
    this.creator = checkNotNull(creator, "creator");
  }

  // TODO: how to reuse this?
  public static Map<Endpoint, ResolvedAddresses> createChildAddressesMap(
      ResolvedAddresses resolvedAddresses) {
    Map<Endpoint, ResolvedAddresses> childAddresses =
        Maps.newLinkedHashMapWithExpectedSize(resolvedAddresses.getAddresses().size());
    for (EquivalentAddressGroup eag : resolvedAddresses.getAddresses()) {
      ResolvedAddresses addresses = resolvedAddresses.toBuilder()
          .setAddresses(Collections.singletonList(eag))
          .setAttributes(Attributes.newBuilder().set(LoadBalancer.IS_PETIOLE_POLICY, true).build())
          .setLoadBalancingPolicyConfig(null)
          .build();
      childAddresses.put(new Endpoint(eag), addresses);
    }
    return childAddresses;
  }

  public void shutdown() {
    logger.log(Level.FINE, "Shutdown");
    for (ChildLbState<K,V> state : childLbStates) {
      state.shutdown();
    }
    childLbStates.clear();
  }

  /** Returns false if presently in the middle of an {@link #acceptResolvedAddresses(Map)} call. */
  public boolean isStateSettled() {
    return !resolvingAddresses;
  }

  /**
   * Update collection to contain only the provided entries. If a key has {@code null} value, then
   * the entry will exist but its load balancer will not receive an address update.
   */
  public Status acceptResolvedAddresses(Map<K, ResolvedAddresses> newChildAddresses) {
    if (resolvingAddresses) {
      // Unlikely to happen because acceptResolvedAddresses() is not normally called by
      // updateBalancingState() implementations.
      throw new IllegalStateException("Reentrancy!");
    }
    resolvingAddresses = true;
    try {
      return acceptResolvedAddressesInternal(newChildAddresses);
    } finally {
      resolvingAddresses = false;
    }
  }

  private Status acceptResolvedAddressesInternal(Map<K, ResolvedAddresses> newChildAddresses) {
    // Create a map with the old values
    Map<K, ChildLbState<K,V>> oldStatesMap =
        Maps.newLinkedHashMapWithExpectedSize(childLbStates.size());
    for (ChildLbState<K,V> state : childLbStates) {
      oldStatesMap.put(state.getKey(), state);
    }

    // Move ChildLbStates from the map to a new list (preserving the new map's order)
    Status status = Status.OK;
    List<ChildLbState<K,V>> newChildLbStates = new ArrayList<>(newChildAddresses.size());
    for (Map.Entry<K, ResolvedAddresses> entry : newChildAddresses.entrySet()) {
      ChildLbState<K,V> childLbState = oldStatesMap.remove(entry.getKey());
      if (childLbState == null) {
        childLbState = new ChildLbState<K,V>(this, entry.getKey());
        childLbState.lb = creator.createChildLb(childLbState);
      }
      newChildLbStates.add(childLbState);
      if (entry.getValue() != null) {
        Status newStatus = childLbState.lb.acceptResolvedAddresses(entry.getValue());
        if (!newStatus.isOk()) {
          status = newStatus;
        }
      }
    }

    childLbStates = newChildLbStates;
    // Even though the picker may still be in use, the common case with hierarchical LBs (with
    // things like `resolvingAddresses` boolean) prevent updates from happening immediately. So
    // even though these LBs may still be in use in the active picker, shutting them down now is not
    // any more racy than normal.

    // Remaining entries in map are orphaned
    for (ChildLbState<K,V> childLbState : oldStatesMap.values()) {
      childLbState.shutdown();
    }
    if (newChildAddresses.isEmpty()) {
      status = Status.UNAVAILABLE.withDescription("No addresses in collection");
    }
    return status;
  }

  public ConnectivityState getAggregateState() {
    if (connectivityCounts[ConnectivityState.READY.ordinal()] > 0) {
      return ConnectivityState.READY;
    }
    if (connectivityCounts[ConnectivityState.CONNECTING.ordinal()] > 0) {
      return ConnectivityState.CONNECTING;
    }
    if (connectivityCounts[ConnectivityState.IDLE.ordinal()] > 0) {
      return ConnectivityState.IDLE;
    }
    return ConnectivityState.TRANSIENT_FAILURE;
  }

  public List<ChildLbState<K,V>> getChildLbStates() {
    return childLbStates;
  }

  /** Returns children in the specified state. */
  public List<ChildLbState<K,V>> getChildLbStates(ConnectivityState targetState) {
    List<ChildLbState<K,V>> children = new ArrayList<>(connectivityCounts[targetState.ordinal()]);
    for (ChildLbState<K,V> child : getChildLbStates()) {
      if (child.getCurrentState() == targetState) {
        children.add(child);
      }
    }
    assert children.size() == connectivityCounts[targetState.ordinal()];
    return children;
  }

  /**
   * This represents the state of load balancer children.  Each endpoint (represented by an
   * EquivalentAddressGroup or EDS string) will have a separate ChildLbState which in turn will
   * have a single child LoadBalancer created from the provided factory.
   *
   * <p>A ChildLbStateHelper is the glue between ChildLbState and the helpers associated with the
   * petiole policy above and the PickFirstLoadBalancer's helper below.
   *
   * <p>If you wish to store additional state information related to each subchannel, then extend
   * this class.
   */
  public static final class ChildLbState<K,V> {
    private final LoadBalancerCollection<K, V> collection;
    private final K key;
    private V value;
    private LoadBalancer lb;
    private ConnectivityState currentState = ConnectivityState.CONNECTING;
    private SubchannelPicker currentPicker =
        new LoadBalancer.FixedResultPicker(LoadBalancer.PickResult.withNoResult());

    private ChildLbState(LoadBalancerCollection<K, V> collection, K key) {
      this.collection = collection;
      this.key = key;
      collection.connectivityCounts[currentState.ordinal()]++;
    }

    private void shutdown() {
      assert currentState != ConnectivityState.SHUTDOWN;
      collection.connectivityCounts[currentState.ordinal()]--;
      this.currentState = ConnectivityState.SHUTDOWN;
      lb.shutdown();
      // FIXME: Is checking value instanceof Closeable better?
      collection.creator.childLbDeleted(this);
      logger.log(Level.FINE, "Child balancer {0} deleted", key);
    }

    @Override
    public String toString() {
      return "Address = " + key
          + ", state = " + currentState
          + ", picker type: " + currentPicker.getClass()
          + ", lb: " + lb;
    }

    public K getKey() {
      return key;
    }

    public void setValue(V value) {
      this.value = value;
    }

    public V getValue() {
      return value;
    }

    public LoadBalancer getLb() {
      return lb;
    }

    public SubchannelPicker getCurrentPicker() {
      return currentPicker;
    }

    public ConnectivityState getCurrentState() {
      return currentState;
    }

    public void updateBalancingState(ConnectivityState newState, SubchannelPicker newPicker) {
      if (currentState == ConnectivityState.SHUTDOWN) {
        return;
      }
      collection.connectivityCounts[currentState.ordinal()]--;
      collection.connectivityCounts[newState.ordinal()]++;
      currentState = newState;
      currentPicker = newPicker;
    }
  }

  /**
   * Endpoint is an optimization to quickly lookup and compare EquivalentAddressGroup address sets.
   * It ignores the attributes. Is used as a key for ChildLbState for most load balancers
   * (ClusterManagerLB uses a String).
   */
  protected static class Endpoint {
    final Collection<SocketAddress> addrs;
    final int hashCode;

    public Endpoint(EquivalentAddressGroup eag) {
      checkNotNull(eag, "eag");

      if (eag.getAddresses().size() < 10) {
        addrs = eag.getAddresses();
      } else {
        // This is expected to be very unlikely in practice
        addrs = new HashSet<>(eag.getAddresses());
      }
      int sum = 0;
      for (SocketAddress address : eag.getAddresses()) {
        sum += address.hashCode();
      }
      hashCode = sum;
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }

      if (!(other instanceof Endpoint)) {
        return false;
      }
      Endpoint o = (Endpoint) other;
      if (o.hashCode != hashCode || o.addrs.size() != addrs.size()) {
        return false;
      }

      return o.addrs.containsAll(addrs);
    }

    @Override
    public String toString() {
      return addrs.toString();
    }
  }
}
