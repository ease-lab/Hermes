------------------------------- MODULE Hermes -------------------------------
EXTENDS     Integers,
            FiniteSets

CONSTANTS   H_NODES,
            H_MAX_VERSION
            
VARIABLES   msgs,
            nodeTS,
            nodeState, 
            nodeRcvedAcks,
            nodeLastWriter,
            nodeLastWriteTS,
            nodeWriteEpochID, 
            aliveNodes,
            epochID 
            
\* all Hermes (+ environment) variables
hvars == << msgs, nodeTS, nodeState, nodeRcvedAcks, nodeLastWriter, 
            nodeLastWriteTS, nodeWriteEpochID, aliveNodes, epochID >>

-------------------------------------------------------------------------------------
HMessage ==  \* Messages exchanged by the Protocol   
    [type: {"INV", "ACK"}, sender    : H_NODES,
                           epochID   : 0..(Cardinality(H_NODES) - 1),
                           version   : 0..H_MAX_VERSION,  
                           tieBreaker: H_NODES] 
    \* Note that we need not send Value w/ INVs, timestamp suffice to check consistency
        \union

    [type: {"VAL"},        \* optimization: epochID is not required for VALs
                           \* epochID   : 0..(Cardinality(H_NODES) - 1),
                           version   : 0..H_MAX_VERSION, 
                           tieBreaker: H_NODES] 

HTypeOK ==  \* The type correctness invariant
    /\  msgs            \subseteq HMessage
    /\ \A n \in H_NODES: nodeRcvedAcks[n] \subseteq (H_NODES \ {n})
    /\  nodeLastWriter  \in [H_NODES -> H_NODES]
    /\  nodeLastWriteTS \in [H_NODES -> [version   : 0..H_MAX_VERSION,
                                       tieBreaker: H_NODES         ]]
    /\  nodeTS          \in [H_NODES -> [version   : 0..H_MAX_VERSION,
                                       tieBreaker: H_NODES         ]]
    /\  nodeState       \in [H_NODES -> {"valid", "invalid", "invalid_write", 
                                         "write", "replay"}]
    \*  membership and epoch id related
    /\  aliveNodes      \subseteq H_NODES
    /\  epochID         \in 0..(Cardinality(H_NODES) - 1)
    /\  nodeWriteEpochID \in [H_NODES -> 0..(Cardinality(H_NODES) - 1)]
                                              

\* The consistent invariant: all alive nodes in valid state should have the same value / TS         
HConsistent == 
    \A k,s \in aliveNodes:  \/ nodeState[k] /= "valid"
                            \/ nodeState[s] /= "valid" 
                            \/ nodeTS[k] = nodeTS[s]
                                              
HInit == \* The initial predicate
    /\  msgs            = {}
    \*  membership and epoch id related
    /\  epochID         = 0
    /\  aliveNodes      = H_NODES
    /\  nodeWriteEpochID = [n \in H_NODES |-> 0]
    \*  Init rest per node replica metadata
    /\  nodeRcvedAcks   = [n \in H_NODES |-> {}]
    /\  nodeState       = [n \in H_NODES |-> "valid"]
    /\  nodeLastWriter  = [n \in H_NODES |-> CHOOSE k \in H_NODES:
                                             \A m \in H_NODES: k <= m]
    /\  nodeTS          = [n \in H_NODES |-> [version |-> 0, 
                                              tieBreaker |-> 
                                              CHOOSE k \in H_NODES: 
                                               \A m \in H_NODES: k <= m]]
    /\  nodeLastWriteTS = [n \in H_NODES |-> [version |-> 0, 
                                              tieBreaker |-> 
                                              CHOOSE k \in H_NODES: 
                                               \A m \in H_NODES: k <= m]]
                                               
-------------------------------------------------------------------------------------

\* A buffer maintaining all network messages. Messages are only appended to this variable (not 
\* removed once delivered) intentionally to check protocols tolerance in dublicates and reorderings 
send(m) == msgs' = msgs \union {m}

\* Check if all acknowledgments for a write have been received                                                  
\*receivedAllAcks(n) == nodeRcvedAcks[n] = H_NODES \ {n}
receivedAllAcks(n) == (aliveNodes \ {n}) \subseteq nodeRcvedAcks[n]
        
equalTS(v1,tb1,v2,tb2) ==  \* Timestamp equality
    /\ v1 = v2
    /\ tb1 = tb2

greaterTS(v1,tb1,v2,tb2) == \* Timestamp comparison
    \/ v1 > v2
    \/ /\   v1 = v2
       /\  tb1 > tb2
       
isAlive(n) == n \in aliveNodes
                   
nodeFailure(n) == \* Emulate a node failure
\*    Make sure that there are atleast 3 alive nodes before killing a node
    /\ Cardinality(aliveNodes) > 2
    /\ aliveNodes' = aliveNodes \ {n}
    /\ epochID'     = epochID + 1
    /\ UNCHANGED <<msgs, nodeState, nodeTS, nodeLastWriter, 
                   nodeLastWriteTS, nodeRcvedAcks, nodeWriteEpochID>>

h_upd_not_aliveNodes ==
    /\  UNCHANGED <<aliveNodes, epochID, nodeWriteEpochID>>
    
    
h_upd_aliveNodes ==
    /\ UNCHANGED <<msgs, nodeState, nodeTS, nodeLastWriter, nodeLastWriteTS, nodeRcvedAcks>>
                   
h_upd_nothing ==                    
    /\ h_upd_not_aliveNodes
    /\ h_upd_aliveNodes
    
-------------------------------------------------------------------------------------

h_upd_state(n, newVersion, newTieBreaker, newState, newAcks) == 
    /\  nodeLastWriter'   = [nodeLastWriter  EXCEPT ![n] = n]
    /\  nodeRcvedAcks'    = [nodeRcvedAcks   EXCEPT ![n] = newAcks]
    /\  nodeState'        = [nodeState       EXCEPT ![n] = newState]
    /\  nodeWriteEpochID' = [nodeWriteEpochID EXCEPT ![n] = epochID] \* we always use the latest epochID
    /\  nodeTS'           = [nodeTS          EXCEPT ![n].version    = newVersion, 
                                                    ![n].tieBreaker = newTieBreaker]
    /\  nodeLastWriteTS'  = [nodeLastWriteTS EXCEPT ![n].version    = newVersion, 
                                                    ![n].tieBreaker = newTieBreaker]
                                            
h_send_inv_or_ack(n, newVersion, newTieBreaker, msgType) ==  
    /\  send([type        |-> msgType,
              epochID     |-> epochID, \* we always use the latest epochID
              sender      |-> n,
              version     |-> newVersion, 
              tieBreaker  |-> newTieBreaker])              

h_actions_for_upd(n, newVersion, newTieBreaker, newState, newAcks) == \* Execute a write
    /\  h_upd_state(n, newVersion, newTieBreaker, newState, newAcks)
    /\  h_send_inv_or_ack(n, newVersion, newTieBreaker, "INV")
    /\  UNCHANGED <<aliveNodes, epochID>>
 

h_actions_for_upd_replay(n, acks) == \* Apply a write-replay using same TS (version, tie-breaker) 
                                     \* and either reset acks or keep already gathered acks
    /\  h_actions_for_upd(n, nodeTS[n].version, nodeTS[n].tieBreaker, "replay", acks)

-------------------------------------------------------------------------------------

HRead(n) ==  \* Execute a read
    /\ nodeState[n] = "valid"
    /\ h_upd_nothing
              
HWrite(n) == \* Execute a write
\*    /\  nodeState[n]      \in {"valid", "invalid"} 
    \* writes in invalid state are also supported as an optimization
    /\  nodeState[n]      \in {"valid"}
    /\  nodeTS[n].version < H_MAX_VERSION \* Only to configurably terminate the model checking 
    /\  h_actions_for_upd(n, nodeTS[n].version + 1, n, "write", {})


HCoordWriteReplay(n) == \* Execute a write-replay after a membership re-config
    /\  nodeState[n] \in {"write", "replay"}
    /\  nodeWriteEpochID[n] < epochID
    /\  ~receivedAllAcks(n) \* optimization to not replay when we have gathered acks from all alive
    /\  h_actions_for_upd_replay(n, nodeRcvedAcks[n])


HRcvAck(n) ==   \* Process a received acknowledment
    \E m \in msgs: 
        /\ m.type     = "ACK"
        /\ m.epochID  = epochID
        /\ m.sender  /= n
        /\ m.sender  \notin nodeRcvedAcks[n]
        /\ equalTS(m.version, m.tieBreaker,
                   nodeLastWriteTS[n].version, 
                   nodeLastWriteTS[n].tieBreaker)
        /\ nodeState[n] \in {"write", "invalid_write", "replay"}
        /\ nodeRcvedAcks' = [nodeRcvedAcks EXCEPT ![n] = 
                                              nodeRcvedAcks[n] \union {m.sender}]
        /\ UNCHANGED <<msgs, nodeLastWriter, nodeLastWriteTS, 
                       aliveNodes, nodeTS, nodeState, epochID, nodeWriteEpochID>>


HSendVals(n) == \* Send validations once acknowledments are received from all alive nodes
    /\ nodeState[n] \in {"write", "replay"}
    /\ receivedAllAcks(n)
    /\ nodeState'         = [nodeState EXCEPT![n] = "valid"]
    /\ send([type        |-> "VAL", 
             version     |-> nodeTS[n].version, 
             tieBreaker  |-> nodeTS[n].tieBreaker])
    /\ UNCHANGED <<nodeTS, nodeLastWriter, nodeLastWriteTS,
                   aliveNodes, nodeRcvedAcks, epochID, nodeWriteEpochID>>
 
HCoordinatorActions(n) ==   \* Actions of a read/write coordinator 
    \/ HRead(n)          
    \/ HCoordWriteReplay(n) \* After failures
    \/ HWrite(n)         
    \/ HRcvAck(n)
    \/ HSendVals(n) 

-------------------------------------------------------------------------------------               
    
HRcvInv(n) ==  \* Process a received invalidation
    \E m \in msgs: 
        /\ m.type     = "INV"
        /\ m.epochID  = epochID
        /\ m.sender  /= n
        \* always acknowledge a received invalidation (irrelevant to the timestamp)
        /\ send([type       |-> "ACK",
                 sender     |-> n,   
                 epochID    |-> epochID,
                 version    |-> m.version,
                 tieBreaker |-> m.tieBreaker])
        /\ IF greaterTS(m.version, m.tieBreaker,
                        nodeTS[n].version, nodeTS[n].tieBreaker)
           THEN   /\ nodeLastWriter' = [nodeLastWriter EXCEPT ![n] = m.sender]
                  /\ nodeTS' = [nodeTS EXCEPT ![n].version    = m.version,
                                          ![n].tieBreaker = m.tieBreaker]
                  /\ IF nodeState[n] \in {"valid", "invalid", "replay"}
                     THEN 
                        nodeState' = [nodeState EXCEPT ![n] = "invalid"]
                     ELSE 
                        nodeState' = [nodeState EXCEPT ![n] = "invalid_write"] 
           ELSE
                  UNCHANGED <<nodeState, nodeTS, nodeLastWriter, nodeWriteEpochID>>
        /\ UNCHANGED <<nodeLastWriteTS, aliveNodes, nodeRcvedAcks, epochID, nodeWriteEpochID>>
     
            
HRcvVal(n) ==   \* Process a received validation
    \E m \in msgs: 
        /\ nodeState[n] /= "valid"
        /\ m.type = "VAL"
        /\ equalTS(m.version, m.tieBreaker,
                   nodeTS[n].version, 
                   nodeTS[n].tieBreaker)
        /\ nodeState' = [nodeState EXCEPT ![n] = "valid"]
        /\ UNCHANGED <<msgs, nodeTS, nodeLastWriter, nodeLastWriteTS,
                       aliveNodes, nodeRcvedAcks, epochID, nodeWriteEpochID>>
   
HFollowerWriteReplay(n) == \* Execute a write-replay when coordinator failed
    /\  nodeState[n] \in {"invalid", "invalid_write"}
    /\  ~isAlive(nodeLastWriter[n])
    /\  h_actions_for_upd_replay(n, {}) 

   
HFollowerActions(n) ==  \* Actions of a write follower
    \/ HRcvInv(n)
    \/ HFollowerWriteReplay(n)
    \/ HRcvVal(n) 
 
------------------------------------------------------------------------------------- 

HNext == \* Hermes (read/write) protocol (Coordinator and Follower actions) + failures
    \E n \in aliveNodes:       
            \/ HFollowerActions(n)
            \/ HCoordinatorActions(n)
            \/ nodeFailure(n) 


H_Spec == HInit /\ [][HNext]_hvars


THEOREM H_Spec =>([]HTypeOK) /\ ([]HConsistent)

=============================================================================
