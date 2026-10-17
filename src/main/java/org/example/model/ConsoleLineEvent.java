package org.example.model;

public record ConsoleLineEvent(String line)
        implements MainEvent {}
