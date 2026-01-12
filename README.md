# Linking Referral Delays to Downstream Utilization and Claims Activity
This is the repository that contains all relevant files for the project "Reducing Referral Leakage Through Workflow &amp; Scheduling Optimization".

## 1. Executive Summary

This project analyzes referral scheduling performance and downstream claims activity to understand where delays occur within the referral workflow and how those delays impact follow-up utilization. The analysis evaluates referrals from order placement through scheduling, visit completion, and downstream claim generation.

**Analysis showed that** **97% of referrals were scheduled within 7 days**, **2.65% of referrals experienced extended delays (>14 days) or were never scheduled**, representing a meaningful at-risk population. By focusing on these metrics, the analysis identifies actionable opportunities to improve access to care through targeted workflow interventions rather than system-wide changes.

---

## 2. Business Problem

**The Problem:** **Referral delays are a hidden driver of access risk and lost follow-up care.**

Although most referrals are scheduled quickly, a small subset experience extended delays or are never scheduled at all. These delayed referrals disproportionately impact key access KPIs such as time to appointment, at-risk referral rates, and downstream utilization, yet they are often masked by strong average performance. As a result, operational teams lack clear visibility into which referrals are driving access risk and where targeted follow-up would be most effective.

**The Solution: This project addresses this gap by identifying and isolating referrals that exceed defined thresholds, enabling focused intervention on the small population responsible for most scheduling risk and reduced downstream utilization rather than broad system-wide workflow changes.**

---

## 3. Methodology

### 3.1 Dataset Used

This project uses a [synthetic electronic health record (EHR) dataset generated with Synthea](https://synthea.mitre.org/downloads), a widely used open-source patient data simulator. The dataset includes longitudinal patient, encounter, procedure, provider, and claims-related data designed to reflect realistic healthcare workflows while containing no real patient information.

### 3.2 Data Modeling and Preparation

The synthetic electronic health record **data was ingested into Azure SQL Server** using Python, Pandas, and pyodbc **and stored in a raw schema**. The raw layer used flexible data types to ensure reliable ingestion and prevent data loss during loading.

Then, **a curated analytics schema was created to support reporting and analysis**. Dimension tables for patients, providers, organizations, and payers were built alongside fact tables for encounters and procedures. Data was cleaned and typed using  TRY_CONVERT to safely cast identifiers, dates, and numeric values while preserving record completeness and avoiding transformation failures.

### 3.3 Referral Workflow Analysis

Referral workflow performance was analyzed by tracking referrals from order placement through scheduling and visit completion. Because the dataset does not include a native referral table, a proxy referral workflow was defined using encounter and procedure data to approximate real-world referral behavior.

The proxy referral workflow was defined as follows:

<img width="1422" height="854" alt="image" src="https://github.com/user-attachments/assets/eab1fbae-6f5f-4002-97e4-879d22af5dad" />

Using this proxy workflow, key timestamps were used to calculate the number of days from referral order to scheduling, which served as the primary access performance metric. **Referrals were grouped into operationally meaningful aging buckets aligned with access thresholds, with referrals delayed 14 days or more classified as at risk.**

Funnel analysis was used to measure referral progression across workflow stages, while cumulative distribution (CDF) analysis evaluated scheduling performance against defined access targets. This approach allowed overall scheduling performance to be assessed alongside long-tail delays that are often obscured by average metrics, providing clear visibility into where referrals stall within the scheduling process.

### 3.4 Claims Analysis

To evaluate downstream utilization, referrals were linked to claims data to assess whether scheduled and completed referrals resulted in follow-up claims activity. Claims were used as a proxy for downstream utilization, allowing the analysis to extend beyond scheduling performance into post-visit outcomes.

For each referral, downstream claims were identified by linking referral-related encounters to associated claim records. Metrics were developed to determine whether a referral generated at least one claim, how many claims were generated per referral, and the time from referral order to the first downstream claim. Referrals with no associated claims were explicitly identified to quantify potential utilization loss.

Claims timing was analyzed using the same threshold-based approach applied to referral scheduling. Time to first claim was grouped into operationally meaningful buckets, and cumulative distribution (CDF) analysis was used to measure how quickly claims were generated following referral orders. This parallel structure enabled direct comparison between scheduling delays and downstream claims behavior, supporting evaluation of whether extended scheduling delays were associated with reduced or delayed utilization.

### 3.5 Visualization and Validation

Interactive Power BI dashboards were then finally built to surface findings in an operationally intuitive manner. Visuals included referral funnels, aging distributions, histograms, and cumulative performance curves. 

All metrics were validated through SQL-based reconciliation to ensure consistency between the source data and dashboard outputs.


**Dashboard 1 - Referral Delay Dashboard**

<img width="1422" height="778" alt="image" src="https://github.com/user-attachments/assets/235e153c-76d5-461f-b648-0c2e974f95d8" />

**Dashboard 2 - Downstream Claims Dashboard**

<img width="1422" height="778" alt="image" src="https://github.com/user-attachments/assets/62f1313f-3e5c-46bc-979f-7efa6ab073af" />

---

## 4. Results and Business Impact

### **4.1 Referral Scheduling Performance**

The analysis shows that overall referral scheduling performance is strong, with the majority of referrals progressing through the workflow efficiently. Approximately **97% of referrals were scheduled within 7 days**, indicating that baseline scheduling processes are functioning well for most patients. Funnel analysis confirmed minimal drop-off between referral order, scheduling, and visit completion for the majority of cases.

<img width="1422" height="554" alt="image" src="https://github.com/user-attachments/assets/6e9c5645-9231-43ed-8e18-de47c3a7920a" />

<img width="1442" height="754" alt="image" src="https://github.com/user-attachments/assets/a4609d8a-a88a-4239-843e-35a03e2faee6" />

However, despite strong average performance, a **small but meaningful subset of referrals experienced extended delays or were never scheduled**. Referrals delayed **14 days or more accounted for approximately 2.65% of total referral volume**, yet represented nearly all scheduling-related access risk. Delays beyond 30 days formed a distinct long-tail population, highlighting where scheduling breakdowns persist.

### **4.2 Identification of At-Risk Referrals**

Aging and cumulative distribution analysis demonstrated that **access risk is driven by long-tail delays rather than system-wide inefficiency**. While most referrals met access targets quickly, referrals exceeding the 14-day threshold consistently accounted for the majority of at-risk cases. This pattern indicates that improvements targeted toward delayed referrals would yield greater impact than broad workflow changes affecting all referrals.

<img width="1374" height="586" alt="image" src="https://github.com/user-attachments/assets/569730bf-63b8-4c2a-8a85-2d9823012480" />

### **4.3 Downstream Claims and Utilization Impact**

Linking referral workflows to downstream claims revealed a clear relationship between scheduling timeliness and follow-up utilization. Referrals scheduled within access targets were substantially more likely to generate downstream claims, while referrals with extended scheduling delays showed reduced claim conversion and longer time to first claim.

<img width="1294" height="580" alt="image" src="https://github.com/user-attachments/assets/85dc03ac-a7ed-4fc1-a523-4696da50758e" />

Referrals classified as at risk generated **fewer downstream claims on average** and were **disproportionately represented among referrals with no associated claims**, suggesting potential utilization loss tied to scheduling delays. These findings indicate that access delays do not stop at scheduling but can propagate into reduced follow-up activity.

<img width="1304" height="552" alt="image" src="https://github.com/user-attachments/assets/5ba1e75e-3f6e-427d-9179-0cbe248a4e19" />

### **4.4 Business Impact and Operational Implications**

Although the overall referral workflow performs well, a **small but operationally significant long-tail cohort** experiences substantial delays. At enterprise scale, a 2.65% at-risk rate translates to **thousands of patients per year** requiring additional follow-up and manual intervention. By focusing on this at-risk population, organizations can improve access and downstream outcomes.

From an operational perspective, the analysis supports prioritizing referral monitoring and escalation workflows focused on delayed referrals rather than implementing broad changes to scheduling processes that are already performing well for most patients.

---

## 5. Business Recommendations

- **Trigger follow-up at 14 days without scheduling:** Review and contact referrals that remain unscheduled at 14 days to prevent them from entering the high-risk delay population.
- **Escalate referrals delayed beyond 30 days:** Route referrals unscheduled after 30 days to an escalation workflow for manual review, alternative scheduling, or provider outreach.
- **Use threshold dashboards for daily exception monitoring:** Begin each day by reviewing threshold-based referral dashboards to identify and prioritize delayed referrals rather than monitoring overall averages.
- **Include claims outcomes in access performance reviews:** Incorporate downstream claims metrics into access reviews to ensure scheduling delays are addressed before follow-up utilization is lost.

---

## 6. Key Takeaways

This analysis demonstrated that **even in a high-performing referral workflow**, a small but operationally significant subset of **referrals can experience delayed or incomplete scheduling**. Although the majority of referrals were scheduled within seven days, **approximately 2.65 percent of referrals required more than 14 days to schedule or were never scheduled**.  

Downstream analysis shows that these delayed referrals are **less likely to generate follow-up claims and tend to experience longer time to utilization**, indicating that scheduling delays extend beyond access metrics and can translate into lost or delayed follow-up care.

<aside>
⚠️

***Assuming there are 100,00 referrals/year, 2.65% are at risk, which translates to 2,650 patients/year. If even 50% of those could be recovered, that would result into ~1,300 additional timely visits, along with corresponding increases in downstream utilization and claims activity.***

</aside>

Therefore, these long-tail delays represent the greatest opportunity for improvement, as they are most likely to impact patient experience, access metrics, downstream utilization, and manual follow-up workload.

Targeted intervention focused on this small at-risk population offers a more effective path to improvement than broad system-wide workflow changes

---

## 7. Next Steps

With additional time and access to operational data, this analysis could be extended in several meaningful ways. 

- Referral workqueue logic could be incorporated to simulate real-time monitoring and escalation workflows, allowing delayed referrals to be flagged automatically as they approach defined access thresholds.
- Segmenting at-risk referrals by referral type or service line would help identify whether certain referral pathways are more prone to delays.
- The analysis could also be expanded to include appointment outcomes such as cancellations and no-shows to better understand repeat delays and scheduling churn.
- On the utilization side, incorporating additional claims attributes or longer follow-up windows could provide a more complete view of downstream activity and potential utilization loss.

**Limitations and assumptions:**

This project relies on a synthetic dataset and uses claims as a proxy for downstream utilization rather than direct clinical outcomes. The referral workflow is approximated using encounter and procedure data due to the absence of a native referral table. Results reflect operational patterns and access performance trends rather than patient-level clinical effectiveness.

---
