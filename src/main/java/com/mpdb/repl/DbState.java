package com.mpdb.repl;

import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Component;

/**
 * State management for debug modes and application settings.
 */
@Getter
@Setter
@Component
public class DbState {

    private boolean debugAstMode = true;

}

