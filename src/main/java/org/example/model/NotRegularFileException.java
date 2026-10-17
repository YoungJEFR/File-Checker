package org.example.model;

import java.io.IOException;

public class NotRegularFileException extends IOException {
    public NotRegularFileException(String message) {
        super(message);
    }
}
