package org.example.model;

public record ServiceTerminatedEvent(long runId, Throwable cause)
        implements MainEvent{}
