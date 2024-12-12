package com.udacity.webcrawler;


import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;

/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {
  private final Clock clock;
  private final PageParserFactory parserFactory;
  private final Duration timeout;
  private final int popularWordCount;
  private final ForkJoinPool pool;
  private final List<Pattern> ignoredUrls;
  private final int maxDepth;

  @Inject
  ParallelWebCrawler(
          Clock clock,
          PageParserFactory parserFactory,
          @Timeout Duration timeout,
          @PopularWordCount int popularWordCount,
          @TargetParallelism int threadCount,
          @IgnoredUrls List<Pattern> ignoredUrls,
          @MaxDepth int maxDepth) {
    this.clock = clock;
    this.parserFactory = parserFactory;
    this.timeout = timeout;
    this.popularWordCount = popularWordCount;
    this.pool = new ForkJoinPool(Math.min(threadCount, Runtime.getRuntime().availableProcessors()));
    this.ignoredUrls = ignoredUrls;
    this.maxDepth = maxDepth;
  }

  @Override
  public CrawlResult crawl(List<String> startingUrls) {
    Instant deadline = clock.instant().plus(timeout);
    ConcurrentMap<String, Integer> counts = new ConcurrentHashMap<>();
    Set<String> visitedUrls = ConcurrentHashMap.newKeySet();

    startingUrls.forEach(url -> pool.invoke(new CrawlTask(url, deadline, maxDepth, counts, visitedUrls)));

    return counts.isEmpty()
            ? new CrawlResult.Builder().setWordCounts(counts).setUrlsVisited(visitedUrls.size()).build()
            : new CrawlResult.Builder().setWordCounts(WordCounts.sort(counts, popularWordCount)).setUrlsVisited(visitedUrls.size()).build();
  }

  private class CrawlTask extends RecursiveTask<Void> {
    private final String url;
    private final Instant deadline;
    private final int depth;
    private final ConcurrentMap<String, Integer> counts;
    private final Set<String> visitedUrls;

    CrawlTask(String url, Instant deadline, int depth, ConcurrentMap<String, Integer> counts, Set<String> visitedUrls) {
      this.url = url;
      this.deadline = deadline;
      this.depth = depth;
      this.counts = counts;
      this.visitedUrls = visitedUrls;
    }

    @Override
    protected Void compute() {
      if (depth == 0 || clock.instant().isAfter(deadline) || visitedUrls.contains(url) || isIgnored(url)) {
        return null;
      }

      visitedUrls.add(url);
      PageParser.Result result = parserFactory.get(url).parse();

      result.getWordCounts().forEach((word, count) -> counts.merge(word, count, Integer::sum));

      List<CrawlTask> subtasks = result.getLinks().stream()
              .map(link -> new CrawlTask(link, deadline, depth - 1, counts, visitedUrls))
              .toList();

      invokeAll(subtasks);
      return null;
    }

    private boolean isIgnored(String url) {
      return ignoredUrls.stream().anyMatch(pattern -> pattern.matcher(url).matches());
    }
  }

  @Override
  public int getMaxParallelism() {
    return Runtime.getRuntime().availableProcessors();
  }
}
