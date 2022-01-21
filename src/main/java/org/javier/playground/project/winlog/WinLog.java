package org.javier.playground.project.winlog;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WinLog {
    String ip;
    String name;
    Long occurTime;
}

