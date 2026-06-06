const fs = require('fs');
const {
  Document, Packer, Paragraph, TextRun, AlignmentType, HeadingLevel,
  ImageRun, Table, TableRow, TableCell, WidthType, BorderStyle, SectionType
} = require('docx');

const TNR = "Times New Roman";

// helper builders -----------------------------------------------------------
const title = (t) => new Paragraph({
  alignment: AlignmentType.CENTER,
  spacing: { after: 120 },
  children: [new TextRun({ text: t, font: TNR, size: 48 })], // 24pt
});
const authors = (lines) => lines.map(l => new Paragraph({
  alignment: AlignmentType.CENTER, spacing: { after: 20 },
  children: [new TextRun({ text: l, font: TNR, size: l.startsWith('DustiniaDelixia')?20:22 })],
}));
const h1 = (n, t) => new Paragraph({
  spacing: { before: 160, after: 80 }, alignment: AlignmentType.LEFT,
  children: [new TextRun({ text: `${n}. ${t}`, font: TNR, size: 20, allCaps: true })],
});
const h2 = (t) => new Paragraph({
  spacing: { before: 100, after: 60 },
  children: [new TextRun({ text: t, font: TNR, size: 20, italics: true })],
});
const body = (t, opts={}) => new Paragraph({
  alignment: AlignmentType.JUSTIFIED, spacing: { after: 80 },
  indent: { firstLine: 220 },
  children: [new TextRun({ text: t, font: TNR, size: 20, ...opts })],
});
const abstractPara = (t) => new Paragraph({
  alignment: AlignmentType.JUSTIFIED, spacing: { after: 80 },
  children: [
    new TextRun({ text: "Abstract—", font: TNR, size: 18, bold: true, italics: true }),
    new TextRun({ text: t, font: TNR, size: 18, italics: true }),
  ],
});
const keywords = (t) => new Paragraph({
  alignment: AlignmentType.JUSTIFIED, spacing: { after: 120 },
  children: [
    new TextRun({ text: "Keywords—", font: TNR, size: 18, bold: true, italics: true }),
    new TextRun({ text: t, font: TNR, size: 18, italics: true }),
  ],
});
const fig = (path, cap) => [
  new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 80 },
    children: [ new ImageRun({ data: fs.readFileSync(path), transformation: { width: 230, height: 162 } }) ] }),
  new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 100 },
    children: [ new TextRun({ text: cap, font: TNR, size: 16 }) ] }),
];

// references
const ref = (n, t) => new Paragraph({
  spacing: { after: 30 }, indent: { left: 220, hanging: 220 },
  children: [new TextRun({ text: `[${n}] ${t}`, font: TNR, size: 18 })],
});

// ---------------------------------------------------------------------------
const single = (children) => ({
  properties: { type: SectionType.CONTINUOUS, column: { count: 1 },
    page: { size: { width: 12240, height: 15840 }, margin: { top: 1080, bottom: 1080, left: 1080, right: 1080 } } },
  children,
});
const dbl = (children) => ({
  properties: { type: SectionType.CONTINUOUS, column: { count: 2, space: 360 } },
  children,
});

const doc = new Document({
  sections: [
    single([
      title("Data-Driven Identification of High-Value Customers and Payment Optimization Opportunities in an Indonesian E-Commerce Marketplace"),
      ...authors([
        "Finance Analytics Team",
        "DustiniaDelixia Groceria — Final Project Lab MCI 2026",
        "Surabaya, Indonesia",
      ]),
      new Paragraph({ spacing: { after: 120 }, children: [] }),
    ]),
    dbl([
      abstractPara("DustiniaDelixia Groceria, a fast-growing Indonesian e-commerce marketplace, has accumulated large volumes of transactional data that remain under-utilised for decision making. This work addresses the Finance area: identifying high-value customers (HVCs), characterising their payment behaviour and geographic distribution, and quantifying revenue currently lost through sub-optimal payment options. We implement a reproducible end-to-end pipeline using Apache Airflow for orchestration, ClickHouse as the analytical warehouse, and Metabase for executive reporting. Analysis of approximately 96 thousand customers and 99 thousand orders shows that the top decile of customers by lifetime spend account for a disproportionate share of revenue, spending roughly 5.6 times more per order than the remaining base. Installment-based orders carry a 64% higher average order value (AOV) than single-payment orders. A conservative uplift model projects recoverable annual revenue on the order of tens of thousands of reais from broadening installment availability. We additionally contribute an interactive revenue simulator that turns the static finding into a tunable, defensible business case."),
      keywords("e-commerce analytics, customer segmentation, payment behaviour, data pipeline, ClickHouse, Airflow, business intelligence"),

      h1("I", "Introduction"),
      body("DustiniaDelixia Groceria connects thousands of small and medium enterprise (SME) sellers with millions of buyers across the archipelago. Four years of rapid growth have produced operational data spanning payments, shipping history, and customer reviews. As competition intensifies and leadership demands investment efficiency, intuition-based decisions are no longer sufficient; data analysis is required to surface missed revenue opportunities."),
      body("This paper focuses on the Finance perspective. A quarterly review revealed a significant gap in average basket value: most customers transact within a normal range, but a distinct segment spends far above average. This high-value segment had never been analysed from the payment angle. Our objectives are to determine who these customers are, what their payment preferences are, where they are located, and where the company forfeits potential profit through unoptimised payment options."),

      h1("II", "Related Background"),
      body("The dataset is adapted from the publicly available Brazilian E-Commerce and Marketing Funnel data published by Olist, restructured for the DustiniaDelixia case study. Customer value segmentation commonly relies on RFM (Recency, Frequency, Monetary) analysis, which ranks customers by purchasing behaviour. Payment flexibility, particularly installment plans, is widely associated with higher willingness to spend, motivating our focus on the relationship between payment method and order value."),

      h1("III", "Methodology"),
      h2("A. Pipeline Architecture"),
      body("We adopt a layered Extract-Load-Transform (ELT) design. Apache Airflow orchestrates an idempotent directed acyclic graph (DAG). Source CSV files are validated and bulk-loaded into a ClickHouse staging schema (dustinia_raw). Dimensional and aggregate models are then materialised into an analytics schema (dustinia_marts). Two hard data-quality gates protect the warehouse: a staging gate enforcing minimum row counts and orphan-record checks, and a marts gate validating that the HVC share falls within an expected band and that no negative order values exist. Metabase connects directly to the marts schema for reporting."),
      h2("B. Dimensional Model"),
      body("The fact table fct_order_payments is grained at one row per order, rolling up payment sequences into a primary payment method, maximum installment count, and an installment-usage flag. The dimension dim_customer aggregates orders to the customer level, computing RFM measures and assigning a value segment. High-value customers are defined as the top decile by lifetime monetary value, with a further top-5% Whale tier."),
      h2("C. Value-Add: Payment Uplift Model"),
      body("For each value segment we compare the AOV of installment orders against non-installment orders. The addressable population is credit-card orders that did not use installments. The projected uplift assumes a conservative adoption rate applied to the observed AOV gap, exposed through an interactive simulator allowing finance stakeholders to vary both the adoption rate and the fraction of the AOV gap realised."),

      h1("IV", "Results"),
      body("After excluding cancelled orders, the cleaned dataset comprises 95,559 unique customers. The top decile (9,563 customers) constitutes the high-value segment. These customers exhibit a mean lifetime spend of R$637 and an AOV of approximately R$607, against R$112 for the remaining base, a ratio of roughly 5.6 to 1."),
      ...fig('fig_aov.png', "Fig. 1. Average lifetime spend by customer value segment."),
      body("Payment method composition within the HVC segment is dominated by credit card, which accounts for approximately 82% of HVC revenue, followed by boleto (bank transfer). Voucher and debit card contribute marginally."),
      ...fig('fig_paymix.png', "Fig. 2. High-value customer revenue share by primary payment method."),
      body("The central finding for Finance concerns installments. Orders that used installments have an AOV of R$198, versus R$121 for single-payment orders, a 64% uplift. This relationship strengthens the case for proactively surfacing installment options at checkout."),
      ...fig('fig_installment.png', "Fig. 3. Average order value: installment versus non-installment orders."),
      body("Geographically, HVC revenue concentrates in the states of SP, RJ, and MG. However, HVC penetration (the share of customers within a state that are high-value) is higher in states such as RS and RJ at above 10%, indicating attractive expansion targets distinct from raw revenue leaders."),
      body("Applying a conservative model, in which 30% of addressable non-installment credit-card orders adopt installments and realise the full segment AOV gap, yields a projected annual uplift of approximately R$84,000. The Standard and Mid-Value segments hold the largest absolute opportunity due to order volume, despite smaller per-order gaps than the Whale tier."),

      h1("V", "Discussion"),
      body("The results challenge the internal assumption that payment behaviour is uniform across the customer base. High-value customers are not only bigger spenders but also heavier users of installment facilities, and the correlation between installment usage and order value suggests payment flexibility is a lever rather than a passive attribute. Because the uplift estimate depends on an adoption assumption, the accompanying simulator is important: it lets decision makers stress-test the figure rather than accept a single point estimate."),
      body("Threats to validity include the correlational nature of the AOV gap; installment users may differ systematically from non-users. The projection should therefore be treated as an upper-bounded hypothesis to be confirmed through a controlled rollout or A/B test."),

      h1("VI", "Conclusion"),
      body("We delivered a reproducible, containerised analytics pipeline that identifies high-value customers, characterises their payment behaviour and location, and quantifies a concrete payment-optimisation opportunity for DustiniaDelixia Groceria. The recommended action is to broaden and more prominently surface installment options for credit-card customers, prioritising high-volume segments, and to validate the projected uplift through a measured experiment. Future work includes incorporating delivery experience and review data to model the full customer lifetime value."),

      h1("", "References"),
      ref(1, "Olist and A. Sionek, \"Brazilian E-Commerce Public Dataset by Olist,\" Kaggle, 2018."),
      ref(2, "Apache Software Foundation, \"Apache Airflow Documentation,\" 2024."),
      ref(3, "ClickHouse Inc., \"ClickHouse: Fast Open-Source OLAP DBMS,\" 2024."),
      ref(4, "Metabase, \"Metabase Business Intelligence Documentation,\" 2024."),
      ref(5, "A. M. Hughes, \"Strategic Database Marketing,\" McGraw-Hill, 2006."),
    ]),
  ],
});

Packer.toBuffer(doc).then(buf => {
  fs.writeFileSync('/home/claude/dustinia_finance/docs/DustiniaDelixia_Finance_Paper.docx', buf);
  console.log('paper written', buf.length, 'bytes');
});
