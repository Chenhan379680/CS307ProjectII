package io.sustc.benchmark;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class BenchmarkReportPage {

    // 页面宽度
    private static final int W = 78;

    public static void print(        BenchmarkConfig cfg,
                                     List<Integer> members,
                                     List<BenchmarkResult> results,
                                     Map<Integer, String> stepDesc) {
        if (results == null) results = Collections.emptyList();

        // 计算总耗时
        long totalMs = 0;
        for (BenchmarkResult r : results) {
            totalMs += safeElapsed(r);
        }

        // 用 id 排序
        List<BenchmarkResult> sorted = new ArrayList<>(results);
        sorted.sort(Comparator.comparingInt(BenchmarkResult::getId));

        // ===== Header =====
        boxTop();
        line(" CS307 Project2 - Benchmark Report");
        line(" Time: " + now() + "   Profile: benchmark");
        line(" Group: " + members.stream().map(String::valueOf).collect(Collectors.joining(", ")));
        line(" DataPath: " + nullSafe(cfg.getDataPath()) + "   student-mode=" + cfg.isStudentMode());
        boxMid();

        // ===== Summary =====
        line(" Summary");
        line(String.format("  - Total steps: %-3d   Total time: %s",
                sorted.size(), fmtMs(totalMs)));

        // 可选：找 Step24（并发）给一个摘要
        BenchmarkResult step24 = sorted.stream().filter(r -> r.getId() == 24).findFirst().orElse(null);
        if (step24 != null) {
            line(String.format("  - Concurrency(step 24): passCnt=%d, elapsed=%s",
                    safePass(step24), fmtMs(safeElapsed(step24))));
        } else {
            line("  - Concurrency(step 24): N/A");
        }
        boxMid();

        // ===== Table =====
        line(" Step Results");
        // 表头
        line(String.format(" %-4s %-38s %-10s %-12s", "ID", "Description", "PassCnt", "Time(ms)"));
        line(" " + "-".repeat(Math.min(W - 2, 70)));

        // 这里我们没法从 Runner 拿到 description（Runner 没保存）
        // 所以展示：Step id + passCnt + time；description 写 "Step <id>"
        for (BenchmarkResult r : sorted) {
            String desc = stepDesc.getOrDefault(r.getId(), "Step " + r.getId());
            String pass = (r.getPassCnt() == null ? "-" : String.valueOf(r.getPassCnt()));
            String time = String.valueOf(safeElapsed(r));
            line(String.format(" %-4d %-38s %-10s %-12s", r.getId(), trim(desc, 38), pass, time));
        }
        boxMid();

        // ===== Notes =====
        line(" Notes");
        line("  - This page is generated in command line for readable report-style display.");
        line("  - Key metrics: passCnt/time per step; concurrency stress test summarized above.");
        boxBottom();
    }

    // ---------- helpers ----------
    private static long safeElapsed(BenchmarkResult r) {
        try { return r.getElapsedTime(); } catch (Exception e) { return 0; }
    }
    private static long safePass(BenchmarkResult r) {
        try {
            if (r.getPassCnt() == null) return 0;
            return r.getPassCnt();
        } catch (Exception e) { return 0; }
    }

    private static String fmtMs(long ms) {
        if (ms < 1000) return ms + "ms";
        double s = ms / 1000.0;
        return String.format("%.2fs", s);
    }

    private static String now() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    private static String nullSafe(String s) {
        return s == null ? "null" : s;
    }

    private static String trim(String s, int max) {
        if (s == null) return "";
        if (s.length() <= max) return s;
        return s.substring(0, Math.max(0, max - 1)) + "…";
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
        if (s == null) s = "";
        if (s.length() > W) s = s.substring(0, W);
        System.out.println("│" + padRight(s, W) + "│");
    }
    private static String padRight(String s, int w) {
        if (s.length() >= w) return s;
        return s + " ".repeat(w - s.length());
    }
}
