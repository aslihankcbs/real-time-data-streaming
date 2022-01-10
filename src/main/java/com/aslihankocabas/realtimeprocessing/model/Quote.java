package com.aslihankocabas.realtimeprocessing.model;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class Quote {
    //current price
    private String c;
    //high price
    private String h;
    //low price
    private String l;
    //open price
    private String o;
    //previous price
    private String pc;
    //time
    private String t;
    private String symbol;
}
