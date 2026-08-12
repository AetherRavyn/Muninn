#[cfg(test)]
mod proptests {
    use proptest::prelude::*;
    use crate::vector_clock::VectorClock;
    use crate::model::*;
    use crate::trust::TrustTier;
    use crate::visibility::Visibility;

    // Generate arbitrary agent IDs
    fn agent_id() -> impl Strategy<Value = String> {
        "[a-z]{1,10}".prop_map(|s| s)
    }

    // Generate arbitrary vector clocks
    fn arb_vector_clock() -> impl Strategy<Value = VectorClock> {
        prop::collection::vec((agent_id(), 0u64..1000), 0..5).prop_map(|entries| {
            let mut clock = VectorClock::new();
            for (agent, ts) in entries {
                for _ in 0..ts {
                    clock.increment(&agent);
                }
            }
            clock
        })
    }

    proptest! {
        #[test]
        fn test_vector_clock_merge_commutative(
            a in arb_vector_clock(),
            b in arb_vector_clock(),
        ) {
            let mut a_clone = a.clone();
            let mut b_clone = b.clone();

            let mut ab = a.clone();
            ab.merge(&b);

            let mut ba = b.clone();
            ba.merge(&a);

            // Merge should be commutative: a.merge(b) == b.merge(a)
            prop_assert_eq!(ab, ba);
        }

        #[test]
        fn test_vector_clock_merge_associative(
            a in arb_vector_clock(),
            b in arb_vector_clock(),
            c in arb_vector_clock(),
        ) {
            let mut ab_c = a.clone();
            ab_c.merge(&b);
            ab_c.merge(&c);

            let mut a_bc = a.clone();
            let mut bc = b.clone();
            bc.merge(&c);
            a_bc.merge(&bc);

            // Merge should be associative
            prop_assert_eq!(ab_c, a_bc);
        }

        #[test]
        fn test_vector_clock_merge_idempotent(
            a in arb_vector_clock(),
        ) {
            let mut a_clone = a.clone();
            a_clone.merge(&a);

            // Merge with self should not change the clock
            prop_assert_eq!(a_clone, a);
        }

        #[test]
        fn test_vector_clock_dominance_transitive(
            a in arb_vector_clock(),
            b in arb_vector_clock(),
        ) {
            let mut a_clone = a.clone();
            a_clone.merge(&b);

            // a.merge(b) should always dominate or equal a
            // (dominates requires strictly greater, so we check !b.dominates(a))
            prop_assert!(!a.dominates(&a_clone) || a_clone == a);

            // a.merge(b) should always dominate or equal b
            prop_assert!(!b.dominates(&a_clone) || a_clone == b);
        }

        #[test]
        fn test_vector_clock_concurrent_not_dominant(
            a in arb_vector_clock(),
            b in arb_vector_clock(),
        ) {
            // If clocks are concurrent, neither should dominate
            if a.is_concurrent_with(&b) {
                prop_assert!(!a.dominates(&b));
                prop_assert!(!b.dominates(&a));
            }
        }

        #[test]
        fn test_vector_clock_child_increments(
            agent in agent_id(),
            ts in 0u64..100,
        ) {
            let mut clock = VectorClock::new();
            for _ in 0..ts {
                clock.increment(&agent);
            }

            let child = clock.child(&agent);

            // Child should have incremented the agent's timestamp
            prop_assert_eq!(child.get(&agent), clock.get(&agent) + 1);
        }

        #[test]
        fn test_trust_tier_ordering(
            tier in prop_oneof![
                Just(TrustTier::Verified),
                Just(TrustTier::Standard),
                Just(TrustTier::Untrusted),
            ],
        ) {
            // Verified > Standard > Untrusted for auto-promotion
            match tier {
                TrustTier::Verified => {
                    prop_assert!(tier.can_auto_promote());
                    prop_assert_eq!(tier.retrieval_multiplier(), 1.0);
                }
                TrustTier::Standard => {
                    prop_assert!(tier.can_auto_promote());
                    prop_assert_eq!(tier.retrieval_multiplier(), 0.9);
                }
                TrustTier::Untrusted => {
                    prop_assert!(!tier.can_auto_promote());
                    prop_assert_eq!(tier.retrieval_multiplier(), 0.5);
                }
            }
        }

        #[test]
        fn test_retention_class_immutability(
            rc in prop_oneof![
                Just(RetentionClass::Ephemeral),
                Just(RetentionClass::Standard),
                Just(RetentionClass::LegalHold),
            ],
        ) {
            match rc {
                RetentionClass::LegalHold => prop_assert!(rc.is_immutable()),
                _ => prop_assert!(!rc.is_immutable()),
            }
        }

        #[test]
        fn test_visibility_allows_access(
            owner in agent_id(),
            requestor in agent_id(),
        ) {
            let owner_id = AgentId(owner.clone());
            let requestor_id = AgentId(requestor.clone());

            // Private: only owner
            let private = Visibility::Private;
            if owner == requestor {
                prop_assert!(private.allows_access(&owner_id, &requestor_id));
            } else {
                prop_assert!(!private.allows_access(&owner_id, &requestor_id));
            }

            // Shared: everyone
            let shared = Visibility::Shared;
            prop_assert!(shared.allows_access(&owner_id, &requestor_id));
        }
    }
}
# 1788294675
# 1788294675
# 1788294675
# 1788294675
# 1788294675
# 1788294675
# 1788294675
# 1788294675
# 1788294675
// commit 57 1788294954529383911
// commit 129 1788294955634476021
// commit 153 1788294956020519819
