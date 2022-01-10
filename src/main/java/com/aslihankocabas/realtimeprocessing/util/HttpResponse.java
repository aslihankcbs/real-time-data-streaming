package com.aslihankocabas.realtimeprocessing.util;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
@AllArgsConstructor
public class HttpResponse {
    private String response;
    private int responseCode;
}
