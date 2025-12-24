package io.sustc.benchmark;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class ConsoleReportPrinter {

    private static final int W = 78;

    public static void printHeader(String title) {
        boxTop();
        line(" " + title);
        line(" Time: " + now());
        boxMid();
    }

    public static void printSectionTitle(String title) {
        line(" " + title);
    }

    public static void printKeyValues(LinkedHashMap<String, String> kv) {
        for (var e : kv.entrySet()) {
            line(String.format("  - %-18s %s", e.getKey() + ":", e.getValue()));
        }
        boxMid();
    }

    public static void printTable(List<String> headers, List<List<String>> rows) {
        // compute col widths
        int cols = headers.size();
        int[] cw = new int[cols];
        for (int i = 0; i < cols; i++) cw[i] = headers.get(i).length();
        for (var r : rows) {
            for (int i = 0; i < cols; i++) {
                cw[i] = Math.max(cw[i], r.get(i).length());
            }
        }

        // clamp total width
        // (简单做法：不做复杂折行，字段不要太长就行)

        // header
        StringBuilder sb = new StringBuilder();
        sb.append(" ");
        for (int i = 0; i < cols; i++) {
            sb.append(pad(headers.get(i), cw[i])).append("  ");
        }
        line(sb.toString().trim());

        // rows
        for (var r : rows) {
            sb.setLength(0);
            sb.append(" ");
            for (int i = 0; i < cols; i++) {
                sb.append(pad(r.get(i), cw[i])).append("  ");
            }
            line(sb.toString().trim());
        }
        boxMid();
    }

    public static void printNotes(List<String> notes) {
        line(" Notes");
        for (String n : notes) {
            line("  - " + n);
        }
        boxBottom();
    }

    // ---------- helpers ----------
    private static String pad(String s, int w) {
        if (s.length() >= w) return s;
        return s + " ".repeat(w - s.length());
    }

    private static void boxTop() {
        System.out.println("┌" + "─".repeat(W) + "┐");
    }
    private static void boxMid() {
        System.out.println("├" + "─".repeat(W) + "┤");
    }
    private static void boxBottom() {
        System.out.println("└" + "─".repeat(W) + "┘");
    }
    private static void line(String s) {
        if (s.length() > W) s = s.substring(0, W);
        System.out.println("│" + pad(s, W) + "│");
    }
    private static String now() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }
}
