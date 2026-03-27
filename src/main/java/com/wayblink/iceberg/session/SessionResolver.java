package com.wayblink.iceberg.session;

import java.util.Objects;

public final class SessionResolver {

  private final SessionStore sessionStore;

  public SessionResolver(SessionStore sessionStore) {
    this.sessionStore = Objects.requireNonNull(sessionStore, "sessionStore");
  }

  public SessionState requireCurrent() {
    return sessionStore.load()
        .orElseThrow(() -> new IllegalStateException("No current target. Run open <path> first."));
  }
}
