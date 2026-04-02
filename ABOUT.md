# Cosmo — When Two AIs Argue About Your Data

Drop a spreadsheet. Ask a question. Watch two AI models debate each other while a galaxy of data points rearranges itself on screen.

That's Cosmo. It's a data analytics tool where the analysts are artificial, they disagree with each other on purpose, and they build their own analysis tools as they go.

## The Problem With AI Analytics

Most AI data tools work like this: you ask a question, the AI gives you an answer, you move on. One perspective. One pass. If the AI has a blind spot, you'll never know.

Cosmo takes a different approach. Instead of one AI, there are two. A primary analyst looks at your data first. Then a companion researcher — a completely different model with a different personality — reads the analyst's findings and pushes back. "You missed this cluster." "That correlation is misleading." "Let me check the outliers you ignored."

They go back and forth. Each round, both agents run real queries against your data, draw their own charts, and steer a live graph visualization to prove their point. You sit back and watch data science happen in real time.

## AIs That Build Their Own Tools

Here's where it gets interesting. The AI agents don't work from a fixed menu of pre-built analyses. They write custom Pandas code on the fly.

Ask "find me something weird in this data" and the AI might:

1. Write a statistical distribution analysis to find which columns have unusual patterns
2. Create a new column called `anomaly_score` by combining several metrics
3. Build a scatter plot showing the outliers
4. Generate graph edges connecting all the anomalous rows to visualize the cluster
5. Save the entire analysis as a reusable plugin called `anomaly_detector`

Next time you upload a different dataset, that plugin is still there. The AI's toolkit grows with every conversation. It's not just analyzing data — it's building a library of specialized analysis tools, shaped by the questions you actually ask.

## The Companion Changes Everything

The companion researcher isn't just a second opinion. You choose its personality:

**Devil's Advocate** picks apart every conclusion. Found a correlation? The devil's advocate checks for confounding variables. See a trend? It looks for the data points that break it.

**Anomaly Hunter** ignores the obvious and dives straight for the weird stuff. The 3 AM data points. The impossible combinations. The rows that don't belong.

**Connector** looks for relationships. It writes code to create graph edges between rows that share values — suddenly your flat spreadsheet becomes a network, and hidden clusters emerge.

**Optimizer** doesn't care about being interesting. It cares about being useful. What can you cut? What should you fix? Where's the waste?

**Storyteller** turns numbers into narrative. Who are the characters in this data? What's the arc?

Each personality produces genuinely different analyses of the same data. Not different phrasing of the same insight — different insights entirely.

## The Graph Is Not Decoration

Most data tools that include a graph view treat it as a static picture. Something pretty for the presentation. In Cosmo, the graph is an instrument the AI actively plays.

When the analyst discovers that 60% of your storage is consumed by three projects, it doesn't just tell you — it flies the camera to those nodes, selects them, and zooms in. When the companion finds a cluster of outliers, it navigates to a completely different region of the graph and highlights the pattern.

Each turn of the debate transforms the visualization. You're not reading about data. You're watching the data reshape itself as two AIs argue about what it means.

## How It Actually Works

You drop a CSV. The system auto-detects what the columns are — which could be IDs, which look like categories for coloring, which are numeric for sizing.

Every row becomes a node in the graph. If you upload a second file, the AI finds the foreign key relationships between them and draws the connections.

Then you talk to it. The AI has Pandas under the hood. Every question triggers real code execution — groupby operations, statistical tests, correlation matrices — and every result gets visualized as charts embedded right in the conversation. Click any chart to blow it up full-screen.

Everything the AI does is transparent. Above each response you can see exactly which tools it called, what Pandas code it wrote, what came back. No black box. If the AI's reasoning is wrong, you can see where.

## Who It's For

Anyone who has a CSV and a question. Data scientists who want a faster first pass. Product managers who want to understand their metrics without writing SQL. Researchers who need to explore a new dataset before committing to a methodology.

Or anyone who just wants to watch two AIs argue about spreadsheets. That part is genuinely entertaining.
