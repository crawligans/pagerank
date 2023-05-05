package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.tools.Hasher;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Scanner;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PageRank {

    private static final Pattern anchorTag = Pattern.compile("<a((\\s+[-\\w]+=\"[^\"]*\")*)\\s*>");
    private static final double d = 0.85;
    private static final double s = 0.15;

    public static void run(FlameContext ctx, String[] args) throws Exception {
        if (args.length < 1) {
            ctx.output("PageRank <t>");
            return;
        }
        double t = Double.parseDouble(args[0]);
        double percent = args.length >= 2 ? Integer.parseInt(args[1]) / 100.0 : 1;
        FlamePairRDD state = ctx.fromTable("crawl", row -> {
            String url = Objects.requireNonNullElse(row.get("url"), "")
                .replaceAll(",", URLEncoder.encode(","));
            try {
                return "%s,1.0,1.0,%s".formatted(normalizeUrl(new URL(url)),
                    new Scanner(Objects.requireNonNullElse(row.get("page"), "")).findAll(anchorTag)
                        .map(m -> m.group(1)).filter(Objects::nonNull)
                        .flatMap(props -> Arrays.stream(props.split("\\s+")))
                        .map(prop -> prop.split("=")).filter(prop -> prop.length >= 2)
                        .filter(prop -> "href".equals(prop[0])).map(prop -> prop[1].strip())
                        .filter(propV -> propV.matches("\".*\""))
                        .map(propV -> propV.substring(1, propV.length() - 1)).map(propV -> {
                            try {
                                return normalizeUrl(new URL(new URL(url), propV)).toString();
                            } catch (MalformedURLException e) {
                                return null;
                            }
                        }).filter(Objects::nonNull).collect(Collectors.joining(",")));
            } catch (MalformedURLException e) {
                return (url.isBlank() ? "null" : url) + ",1.0,1.0,";
            }
        }).mapToPair(e -> {
            String[] s = e.split(",", 2);
            return new FlamePair(s[0], s[1]);
        });

        int iterations = 0;
        FlameRDD diffs;
        double n_converged = 0;
        do {
            iterations++;
            FlamePairRDD invRank = state.flatMapToPair(p -> {
                String[] rankSplit = p._2().split(",", 3);
                if (rankSplit.length <= 2) {
                    return Collections::emptyIterator;
                }
                String[] links = rankSplit[2].split(",");
                return () -> Stream.concat(Arrays.stream(links).map(l -> {
                        try {
                            return normalizeUrl(new URL(l)).toString();
                        } catch (MalformedURLException e) {
                            return null;
                        }
                    }).distinct().filter(Objects::nonNull).map(l -> new FlamePair(l,
                        String.valueOf(d * Double.parseDouble(rankSplit[0]) / links.length))),
                    Stream.of(new FlamePair(p._1(), "0.0"))).iterator();
            });
            FlamePairRDD sumedRanks = invRank.foldByKey("0.0",
                (v1, v2) -> String.valueOf(Double.parseDouble(v1) + Double.parseDouble(v2)));
            invRank.drop();
            FlamePairRDD joined = sumedRanks.join(state);
            sumedRanks.drop(true);
            state.drop(true);
            state = joined.flatMapToPair(p -> {
                String[] split = p._2().split(",", 4);
                return () -> Collections.singleton(new FlamePair(p._1(),
                    String.join(",", String.valueOf(Double.parseDouble(split[0]) + s), split[1],
                        split[3]))).iterator();
            });
            joined.drop();
            diffs = state.flatMap(p -> {
                String[] split = p._2().split(",", 3);
                return () -> Collections.singleton(String.valueOf(
                        Math.abs(Double.parseDouble(split[0]) - Double.parseDouble(split[1]))))
                    .iterator();
            });
            n_converged = Double.parseDouble(
                diffs.flatMap(v -> Collections.singleton(Double.parseDouble(v) < t ? "1" : "0"))
                    .fold("0",
                        (v1, v2) -> String.valueOf(Integer.parseInt(v1) + Integer.parseInt(v2))));
        } while (n_converged <= diffs.count() * percent - 1);

        ctx.getKVS().persist("pageranks");
        state.flatMapToPair(p -> {
            String[] split = p._2().split(",", 2);
            String hash = Hasher.hash(p._1());
            ctx.getKVS().put("pageranks", hash, "url", p._1());
            ctx.getKVS().put("pageranks", hash, "rank", split[0]);
            return Collections::emptyIterator;
        });

        ctx.output("# iters: " + iterations);
    }

    public static URL normalizeUrl(URL url) throws MalformedURLException {
        String protocol = url.getProtocol();
        String host = url.getHost();
        int port = url.getPort() < 0 ? url.getDefaultPort() : url.getPort();
        String file = url.getPath();
        return new URL(protocol, host, port, "".equals(file) ? "/" : file);
    }
}
