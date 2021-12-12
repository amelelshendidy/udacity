package com.udacity.webcrawler;

import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.RecursiveAction;
import java.util.regex.Pattern;

public class CrawlInternalTask extends RecursiveAction {
    private Clock clock;
    private PageParserFactory parserFactory;
    private final String url;
    private final Instant deadline;
    private final int maxDepth;
    private final Map<String, Integer> counts;
    private final Set<String> visitedUrls;
    private final List<Pattern> ignoredUrls;

    public CrawlInternalTask(Clock clock, PageParserFactory parserFactory, String url, Instant deadline, int maxDepth, Map<String, Integer> counts, Set<String> visitedUrls, List<Pattern> ignoredUrls) {
        this.clock = clock;
        this.parserFactory = parserFactory;
        this.url = url;
        this.deadline = deadline;
        this.maxDepth = maxDepth;
        this.counts = counts;
        this.visitedUrls = visitedUrls;
        this.ignoredUrls = ignoredUrls;
    }

    

    public String getUrl() {
        return url;
    }

    public Instant getDeadline() {
        return deadline;
    }

    public int getMaxDepth() {
        return maxDepth;
    }

    public Map<String, Integer> getCounts() {
        return counts;
    }

    public Set<String> getVisitedUrls() {
        return visitedUrls;
    }

    public List<Pattern> getIgnoredUrls() {
        return ignoredUrls;
    }

    @Override
    protected void compute() {
        if (maxDepth == 0 || clock.instant().isAfter(deadline)) {
        return;
        }
        for (Pattern pattern : ignoredUrls) {
            if (pattern.matcher(url).matches()) {
                return;
            }
        }
        if (visitedUrls.contains(url)) {
            return;
        }
        visitedUrls.add(url);
        PageParser.Result result = parserFactory.get(url).parse();
        for (Map.Entry<String, Integer> e : result.getWordCounts().entrySet()) {
            if (counts.containsKey(e.getKey())) {
                counts.put(e.getKey(), e.getValue() + counts.get(e.getKey()));
            } else {
                counts.put(e.getKey(), e.getValue());
            }
        }

        List<CrawlInternalTask> subTasks = new ArrayList<>();
        for (String link : result.getLinks()) {
            CrawlInternalTask subTask = new CrawlInternalTask.Builder()
                    .setClock(clock)
                        .setParserFactory(parserFactory)
                            .setUrl(link)
                                .setDeadline(deadline)
                                    .setMaxDepth(maxDepth - 1)
                                            .setCounts(counts)
                                                    .setVisitedUrls(visitedUrls)
                                                            .build();
            subTasks.add(subTask);
        }

        invokeAll(subTasks);
    }

    public static final class Builder {
        private Clock clock;
        private PageParserFactory parserFactory;
        private String url;
        private Instant deadline;
        private int maxDepth;
        private Map<String, Integer> counts = new Hashtable<>();
        private Set<String> visitedUrls = new HashSet<>();
        private List<Pattern> ignoredUrls = new ArrayList<>();

        public Builder setClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder setParserFactory(PageParserFactory parserFactory) {
            this.parserFactory = parserFactory;
            return this;
        }

        public Builder setUrl(String url) {
            this.url = url;
            return this;
        }

        public Builder setDeadline(Instant deadline) {
            this.deadline = deadline;
            return this;
        }

        public Builder setMaxDepth(int maxDepth) {
            this.maxDepth = maxDepth;
            return this;
        }

        public Builder setCounts(Map<String, Integer> counts) {
            this.counts = counts;
            return this;
        }

        public Builder setVisitedUrls(Set<String> visitedUrls) {
            this.visitedUrls = visitedUrls;
            return this;
        }

        public Builder setIgnoredUrls(List<Pattern> ignoredUrls) {
            this.ignoredUrls = ignoredUrls;
            return this;
        }

        public CrawlInternalTask build() {
            return new CrawlInternalTask(clock, parserFactory, url, deadline, maxDepth, counts, visitedUrls, ignoredUrls);
        }
    }


}
