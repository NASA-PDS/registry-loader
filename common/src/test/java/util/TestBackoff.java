package util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import gov.nasa.pds.registry.common.util.Backoff;

public class TestBackoff {

  @Test
  void delay_doublesEachAttempt_untilCap() {
    Backoff backoff = new Backoff(10_000L, 300_000L, 0.0);

    assertEquals(10_000L, backoff.delayMillis(0));
    assertEquals(20_000L, backoff.delayMillis(1));
    assertEquals(40_000L, backoff.delayMillis(2));
    assertEquals(80_000L, backoff.delayMillis(3));
    assertEquals(160_000L, backoff.delayMillis(4));
    assertEquals(300_000L, backoff.delayMillis(5), "320s must be capped at 300s");
    assertEquals(300_000L, backoff.delayMillis(6));
  }

  @Test
  void delay_largeAttemptNumbers_doNotOverflow() {
    Backoff backoff = new Backoff(10_000L, 300_000L, 0.0);

    assertEquals(300_000L, backoff.delayMillis(31));
    assertEquals(300_000L, backoff.delayMillis(75));
    assertEquals(300_000L, backoff.delayMillis(Integer.MAX_VALUE));
  }

  @Test
  void delay_negativeAttempt_treatedAsFirst() {
    Backoff backoff = new Backoff(1_000L, 30_000L, 0.0);

    assertEquals(1_000L, backoff.delayMillis(-3));
  }

  @Test
  void delay_jitterOnlyShortensAndStaysWithinBounds() {
    Backoff backoff = new Backoff(10_000L, 300_000L, 0.25);

    // random = 0 -> no reduction; random -> 1 -> up to 25% reduction
    assertEquals(40_000L, backoff.delayMillis(2, 0.0));
    assertEquals(30_000L, backoff.delayMillis(2, 1.0));
    for (int i = 0; i < 1000; i++) {
      long delay = backoff.delayMillis(2);
      assertTrue(delay >= 30_000L && delay <= 40_000L, "delay out of range: " + delay);
    }
    for (int attempt = 0; attempt < 100; attempt++) {
      assertTrue(backoff.delayMillis(attempt) <= 300_000L, "jittered delay must never exceed the cap");
    }
  }

  @Test
  void constructor_rejectsInvalidArguments() {
    assertThrows(IllegalArgumentException.class, () -> new Backoff(0, 10, 0.0));
    assertThrows(IllegalArgumentException.class, () -> new Backoff(10, 5, 0.0));
    assertThrows(IllegalArgumentException.class, () -> new Backoff(1, 10, -0.1));
    assertThrows(IllegalArgumentException.class, () -> new Backoff(1, 10, 1.1));
  }

  @Test
  void pause_interrupted_returnsFalseAndRestoresInterruptFlag() {
    Thread.currentThread().interrupt();
    try {
      assertFalse(Backoff.pause(10_000L));
      assertTrue(Thread.currentThread().isInterrupted(), "interrupt flag must be restored");
    } finally {
      Thread.interrupted(); // clear the flag so it does not leak into other tests
    }
  }

  @Test
  void pause_completes_returnsTrue() {
    assertTrue(Backoff.pause(1L));
  }
}
