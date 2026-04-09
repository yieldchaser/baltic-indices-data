---
category: book
commodities:
- iron_ore
- coal
- crude_oil
- gas
date: null
doc_id: book_predictability_of_second_hand_bulk_carriers_with_a_novel_hybrid
document_type: reference_book
key_entities:
- Australia
keywords:
- capesize
- panamax
- supramax
- handysize
- vlcc
- container
- australia
- iron_ore
- coal
- crude_oil
- gas
- forecasting
market_tone: constructive
regions:
- australia
section_count: 5
signals: {}
source: book
source_path: reports/Predictability of second-hand bulk carriers with a novel hybrid.pdf
summary: 'of Pages 10

  The Asian Journal of Shipping and Logistics xxx (xxxx) xxx-xxx

  Contents lists available at ScienceDirect

  The Asian Journal of Shipping and

  HOSTED BY

  Logistics

  j ourna l h omepage: www.elsevier.com/locate/ajsl

  Predictability of second-hand bulk carriers with a novel hybrid

  algorithm

  Okan Durua, Emrah Gulayb, Sinem Celik Girginc,∗

  aResearch & Development, Ocean Dynamex Inc., Ottawa, ON, Canada

  bDepartment of Econometrics, Dokuz Eylul University, Turkey

  cMaritime and Logistics Management Department, University of Tasmania-Australian
  Maritime College, Launceston, Australia

  a r t i c l e i n f o

  Article history:

  Received 29 January 2021

  Received in revised form 2 July 2021

  Accepted 21 July 2021

  Keywords:

  Investment timing

  Predictability

  Lead-lag structure

  Shipping Q

  a

  index

  b s t r a c t

  This paper investigates the predictability of the asset prices of commodity transport
  (i.e. dry bulk carriers)

  by testing the shipping Q index as a leading indicator. We employ a comprehensive
  back-testing proce-

  dure with a broad spectrum of benchmark simulations.'
themes:
- capesize
- panamax
- supramax
- handysize
- vlcc
- container
title: Predictability of second-hand bulk carriers with a novel hybrid
vessel_classes:
- capesize
- panamax
- supramax
- handysize
- vlcc
- container
---

## Summary
of Pages 10
The Asian Journal of Shipping and Logistics xxx (xxxx) xxx-xxx
Contents lists available at ScienceDirect
The Asian Journal of Shipping and
HOSTED BY
Logistics
j ourna l h omepage: www.elsevier.com/locate/ajsl
Predictability of second-hand bulk carriers with a novel hybrid
algorithm
Okan Durua, Emrah Gulayb, Sinem Celik Girginc,∗
aResearch & Development, Ocean Dynamex Inc., Ottawa, ON, Canada
bDepartment of Econometrics, Dokuz Eylul University, Turkey
cMaritime and Logistics Management Department, University of Tasmania-Australian Maritime College, Launceston, Australia
a r t i c l e i n f o
Article history:
Received 29 January 2021
Received in revised form 2 July 2021
Accepted 21 July 2021
Keywords:
Investment timing
Predictability
Lead-lag structure
Shipping Q
a
index
b s t r a c t
This paper investigates the predictability of the asset prices of commodity transport (i.e. dry bulk carriers)
by testing the shipping Q index as a leading indicator. We employ a comprehensive back-testing proce-
dure with a broad spectrum of benchmark simulations.

## G Model
ARTICLE IN PRESS
AJSL-280; No. of Pages 10
The Asian Journal of Shipping and Logistics xxx (xxxx) xxx-xxx
Contents lists available at ScienceDirect
The Asian Journal of Shipping and
HOSTED BY
Logistics
j ourna l h omepage: www.elsevier.com/locate/ajsl
Predictability of second-hand bulk carriers with a novel hybrid
algorithm
Okan Durua, Emrah Gulayb, Sinem Celik Girginc,∗
aResearch & Development, Ocean Dynamex Inc., Ottawa, ON, Canada
bDepartment of Econometrics, Dokuz Eylul University, Turkey
cMaritime and Logistics Management Department, University of Tasmania-Australian Maritime College, Launceston, Australia
a r t i c l e i n f o
Article history:
Received 29 January 2021
Received in revised form 2 July 2021
Accepted 21 July 2021
Keywords:
Investment timing
Predictability
Lead-lag structure
Shipping Q
a
index
b s t r a c t
This paper investigates the predictability of the asset prices of commodity transport (i.e. dry bulk carriers)
by testing the shipping Q index as a leading indicator. We employ a comprehensive back-testing proce-
dure with a broad spectrum of benchmark simulations. The shipping Q index (an adaptation of Tobin's Q
index) has been introduced to benchmark models to observe predictive gain and interpret predictability
features. This study presents a novel hybrid model to forecast time series data. The forecasting ability
of the proposed hybrid algorithm is compared to specific univariate time series models, dynamic mod-
els, nonlinear models, and widely used hybrid models in the literature. The findings document that not
only the proposed hybrid model performs better than the other competitive models in terms of hold out
sample forecasting, but also using the shipping Q index improves the forecast accuracy by remarkably
reducing forecasting error.
© 2021 The Author. Production and hosting by Elsevier B.V. This is an open access article under the CC
BY-NC-ND license (http://creativecommons.org/licenses/by-nc-nd/4.0/).
1. Introduction
Dry cargo ships are designed and operated in this supply chain
for centuries. Based on the cargo capacity of ships, there are four
major size groups namely Handysize (20k-45k dwt1), Handymax
(45k-60k dwt), Panamax2(60k-90k dwt) and Capesize (90k + dwt,
mostly 120k-150k). Two primary raw materials, iron ore and coal
(both coking coal and steam coal) are usually carried on Cape-
size bulk carriers over 120k metric tons of parcel size. Investors
in the shipping business need to allocate large amounts of capital
on acquiring new and second-hand assets (i.e. ships) to employ in
major commodity trades and gain revenue streams in operating
income (based on freight rate). However, ships are not only cash-
generating units through operating income, but their asset value
may also cause gain/loss in oscillating ship markets. In this regard,
the 'asset play' strategy (revenues on buy-sell spread) is an essential
component of the portfolio management in the shipping corpora-
∗
Corresponding author.
E-mail address: sinemcelikgirgin@utas.edu.au (S.C. Girgin).
1 Deadweight tonnage (dwt) refers to the carrying capacity including cargo, fuel,
water or crew. Particularly in larger ship size, dwt is almost the cargo carrying
capacity.
2 Literally it means the maximum size that can pass through Panama
tions,
Canal.
and it is contingent upon the investment timing (i.e. temporal
arbitrage) by its nature similar to conventional financial assets.
The revenue model of ship owning business has two major
income streams: (1) Operating revenue from chartering operations
and (2) impairment gains generated from changes in ship prices.
The latter component of the revenue model can lead to a massive
gain or loss due to the investment/divestment timing and the qual-
ity of asset value monitoring. Ship prices in the Capesize tonnage
can be anywhere between USD 30-140 million considering the
asset market of the last few decades, and asset prices may undergo
intense volatility at the gain/loss of over USD 50 million. In June
2008, a large Capesize (120k + metric ton capacity) was sold at USD
140 million, and the same asset was priced at just USD 40 million
in November 2008. An investment in June 2008 has lost USD 100
million through impairment in such a short period of time. That
level of value has never returned back as of the data of this paper
(January 2020). In this regard, asset value monitoring is now an
essential part of portfolio management in ship investments. It is
also a critical factor in the bank credit analysis as the ship mort-
gages which are usually monitored through the minimum security
value covenant against asset value shortfall (Duru, 2018, p. Chapter
8) and financial auditors pay particular attention as a major source
of impairment losses.
According to a study conducted at the Harvard Business School
(Greenwood and Hanson, 2015), shipping asset prices are signif-
icantly predictable due to the mistiming of investments as the
https://doi.org/10.1016/j.ajsl.2021.07.002
2092-5212 © 2021 The Author. Production and hosting by Elsevier B.V. This is an open access article under the CC BY-NC-ND license (http://creativecommons.org/licenses/
by-nc-nd/4.0/).
Please cite this article as: Duru, O., et al, Predictability of second-hand bulk carriers with a novel hybrid algorithm, The Asian Journal of
Shipping and Logistics, https://doi.org/10.1016/j.ajsl.2021.07.002

## G Model
ARTICLE IN PRESS
AJSL-280; No. of Pages 10
O. Duru et al. The Asian Journal of Shipping and Logistics xxx (xxxx) xxx-xxx
Fig. 1. The proposed hybrid algorithm.
2.2.1. Linear forecasting models
There are a number of different econometric models in the lit-
erature proposed by researchers to predict the future movement of
freight rates. Among those, the ARIMA model by Box and Jenkins
(1976), the VAR model by Sims (1980), VECM by Engle and Granger
(1987) were widely used, linear forecasting models. Franses and
Veenstra (1997) proposed the VAR model to forecast Bulk Dry Index
(BDI). Cullinane and Khanna (1999) employed the ARIMA model
to forecast the Baltic Freight Index (BFI) dataset. Batchelor et al.
(2007) concluded that ARIMA and VAR models provide better fore-
cast accuracy than the VECM model for the Baltic Panamax Index.
Chen et al. (2012) performed four different models, such as VAR,
VARX, ARIMA, and ARIMAX, in the dry bulk market. They showed
that VAR and VARX models perform compared to the others.
2.2.2. Nonlinear forecasting models
If the price series is linear, the models in question could gen-
erate useful results in terms of forecasting. However, forecasting
becomes a challenging task because of the existence of nonlinear-
ity in bulk shipping price series. Thus, as an artificial intelligence
model, neural networks (ANN) and support vector machines (SVM)
have been widely applied successfully in the bulk shipping market
(Li and Parsons, 1997) carried out the comparison of artificial neural
networks (ANNs) and ARMA models. The findings of a comparative
analysis of ANNs and ARMA models showed that the ANNs model
outperformed the ARMA model. Lyridis et al. (2004) suggested a
nonlinear modelling framework by using the ANNs model to fore-
cast Very Large Crude Carriers (VLCC). Thalassinos et al. (2013)
focused on the nonlinear analysis approach, such as False Near-
est Neighbors (FNN), to forecast the BDI. Uyar and I˙lhan (2016)
forecasted annual freight rates by using a recurrent fuzzy neu-
ral network. They emphasised the superiority of their
2.2.3.
proposed
approach.
Hybrid forecasting models
More recently, hybrid forecasting models have been extensively
applied to combine linear and nonlinear models because they can
have superior capabilities to deal with some weaknesses in the
forecasting field when compared to traditional forecasting mod-
els. Han et al. (2014) employed wavelet transform to denoise the
BDI series and proposed the algorithm that combined the model of
wavelet transform and support vector machine (SVM). Zeng et al.
(2016) contributed to knowledge in respect of improving the fore-
cast accuracy by using Empirical Mode Decomposition (EMD). The
BDI series was decomposed into several independent instinct mode
functions (IMFs). In this context, each component was modelled
by using ANN. It was concluded that the proposed methodology
that was EMD-ANN approach led to improved forecasting perfor-
mance rather than the VAR model (based on out-sample results).
Guan et al. (2016) forecasted the Baltic Supramax Index by using
hybrid multi-step SVM. Eslami et al. (2017) proposed a new hybrid
forecasting approach that combined the ANN model and adap-
tive genetic algorithm (AGA) to improve forecast accuracy. They
found that the proposed hybrid model performed better forecast-
ing results by providing smaller mean square error (MSE) than the
regression model, moving average (MA) model, and ANN model.
3. Methodology and data
3.1. Shipping Q as an adaptation of Tobin's Q index
SQ index, in other words, a momentum indicator for shipping
asset prices, created following the fundamentals of capital invest-
ment model, Tobin Q theory (Brainard and Tobin, 1968; Tobin,
1969). In Tobin Q model, dynamic value change of assets through-
out the time captured by the ratio of market value of a firm to
replacement value. The Q ratio aimed to identify if the firm's value
is over-valued, when the ratio is greater than 1, or under-valued,
when the ratio is lower than 1. In Tobin Q theory, if ratio is greater
3

## G Model
ARTICLE IN PRESS
AJSL-280; No. of Pages 10
O. Duru et al. The Asian Journal of Shipping and Logistics xxx (xxxx) xxx-xxx
Fig. 2. Training, validation, and test sets of dry bulk carriers: (a) 5 years old, (b) 10 years old, and (c) 15 years old.
Fig. 3. Differences in ranked order means of models: (a) 5 years old dry bulk carrier, (b) 10 years old dry bulk carrier, and (c) 15 years old dry bulk carrier.
4

## G Model
ARTICLE IN PRESS
AJSL-280; No. of Pages 10
O. Duru et al. The Asian Journal of Shipping and Logistics xxx (xxxx) xxx-xxx
Fig. 5. Second-hand dry bulk carriers 10 years-old: (a) Capesize, (b) Handymax, (c)
Handysize, and (d) Panamax.
not practically useful if the security shortfall arises as a result of the
market collapse and asset bubbles. In this regard, the SQ index can
be utilised in identifying overpriced assets and potential security
value shortfalls during the credit analysis stage.
5. Conclusion
This paper proposed a novel hybrid algorithm for second-
hand bulk carriers forecasting, and ARDL-EMD-ANN hybrid model
developed to test forecasting performance. The modelling and fore-
casting steps were described, and the empirical analysis was carried
out based on various type of carriers. The forecasting accuracy of
the hybrid model was compared with the competitive models in
most of the forecasting studies. The results indicated that the pro-
posed algorithm that uses the strength of the combination of the
ARDL and ANN models by including the decomposition part for
modelling residuals were superior to all benchmark models.
The proposed novel model applied to the SQ index sample data,
which was tested and validated as a leading index for invest-
ment timing in a given set of dry bulk assets, which developed
information for the market entry-exit mechanism. In limited use,
the SQ index can improve the long-term return of investment
by optimising the asset play components. In massive use, antici-
pated dynamics would be negated by investors, and the leading
impact may have deteriorated. Eventually, the empirical analysis
proved that the proposed hybrid algorithm is a viable alternative
for second-hand bulk carriers forecasting, and it can be applied to
different time series data in other
Fig.
areas.
6. Second-hand dry bulk carriers 15 years old: (a) Capesize, (b) Handymax, (c)
Handysize, and (d) Panamax.
Moreover, utilised empirical mode decomposition (EMD) and
feedforward neural networks (artificial neural network-ANN)
stated that the SQ index has a crucial role in explaining market entry
and exit decisions. Practically, we can state that this index can be
used by market players, charterers, shipping management compa-
nies. Using this index in the decision-making process can lead to
several benefits, forecasting market conditions can facilitate taking
a position in the market, buy, or sell.
SQ index is a dynamic momentum indicator for shipping asset
prices, which re-evaluates the market entry and exit decisions, as
the freight rate changes. In earlier studies, the SQ index presented
solely dynamic index, not implemented in parametric linear or non-
linear predictive models as an explanatory variable. In this study, it
was analysed as an explanatory variable and tested its forecasting
performance. In the extended literature, historical asset prices and
freight rates are utilized as an explanatory variable. On the other
hand, the raw dataset does not reflect asset price misvaluation
which is very common in the industry. Asset misvaluation typi-
cally cause asset value shortfalls (minimum value covenant) and
trigger foreclosure or restructuring of the loan facility. In this novel
study, we implemented the SQ index to represent the misvaluation
as a momentum indicator.
One-step ahead predictive performance in this study leads the
future research on the optimisation of lead-lag structure for var-
ious methodologies as well as blending with other explanatory
variables.
9

## G Model
ARTICLE IN PRESS
AJSL-280; No. of Pages 10
O. Duru et al. The Asian Journal of Shipping and Logistics xxx (xxxx) xxx-xxx
Author declaration
There is no conflict of interest to declare. This manuscript has
not been published or resented elsewhere in part or in entirety and
is not under consideration by another journal. We have read and
understood your journal's policies, and we believe that neither the
manuscript nor the study violates any of these.
Declarations of interest
None.
References
Alizadeh, A., & Nomikos, N. (2007). Investment timing and trading strategies in the
sale and purchase market for ships. Transportation Research Part B Methodologi-
cal, 41(1), 126-143.
Batchelor, R., Alizadeh, A., & Visvikis, I. (2007). Forecasting spot and forward prices
in the international freight market. International Journal of Forecasting, 23(1),
101-114.
Box, G. E. P., & Jenkins, G. M. (1976). Time series analysis: Forecasting and control.
Holden-Day.
Brainard, W., & Tobin, J. (1968). Pitfalls in financial model building. The American
Economic Review, 58, 99-122.
Celik Girgin, S., Karlis, T., & Duru, O. (2019). Valuation mismatch and shipping q
indicator for shipping asset management. Maritime Policy & Management, 1-18.
Chen, S., Meersman, H., & Van de Voorde, E. (2012). Forecasting spot rates at main
routes in the dry bulk market. Maritime Economics & Logistics, 14(4), 498-537.
Cullinane, K., & Khanna, M. (1999). Economies of scale in large container ships.
Journal of Transport Economics and Policy, 33(2), 185-207.
Dikos, G., & Marcus, H. (2003). The term structure of second-hand prices: A structural
partial equilibrium model. Maritime Economics & Logistics, 5(3), 251-267.
Duru, O. (2013). Irrational exuberance, overconfidence and short-termism:
Knowledge-to-action asymmetry in shipping asset management. The Asian Jour-
nal of Shipping and Logistics, 29(1), 43-58.
Duru, O. (2018). Shipping business unwrapped: Illusion, Bias and fallacy in the shipping
business. Routledge.
Engle, R. F., & Granger, C. W. (1987). Co-integration and error correction: Represen-
tation, estimation, and testing. Econometrica: Journal of the Econometric Society,
251-276.
Eslami, P., Jung, K., Lee, D., & Tjolleng, A. (2017). Predicting tanker freight rates using
parsimonious variables and a hybrid artificial neural network with an adaptive
genetic algorithm. Maritime Economics & Logistics, 19(3), 538-550.
Forrester, J. W. (1958). Industrial dynamics. A major breakthrough for decision mak-
ers. Harvard business review, 36(4), 37-66.
Franses, P. H., & Veenstra, A. (1997). A cointegration approach to forecasting freight
rates in the dry bulk shipping sector. Transportation Research Part A: Policy &
Practice, 447-458.
Girgin, S. C., Karlis, T., & Nguyen, H.-O. (2018). A critical review of the literature on
firm-level theories on ship investment. International Journal of Financial Studies,
6(11).
Greenwood, R., & Hanson, S. G. (2015). Waves in ship prices and investment. The
Quarterly Journal of Economics, 130(1), 55-109.
Guan, F., Peng, Z., Wang, K., Song, X., & Gao, J. (2016). Multi-step hybrid predic-
tion model of baltic supermax index based on support vector machine. Neural
Network World, 26(3),
Han,
219.
Q., Yan, B., Ning, G., & Yu, B. (2014). Forecasting dry bulk freight index with
improved SVM. Mathematical Problems in Engineering, 2014.
Jeon, J.-W., Duru, O., & Yeo, G.-T. (2020). Modelling cyclic container freight index
using system dynamics. Maritime Policy & Management, 47(3), 287-303.
Kaboudan, M. (2001). Compumetric forecasting of crude oil prices. In Paper Presented
at the Proceedings of the 2001 Congress on Evolutionary Computation (IEEE Cat. No.
01TH8546).
Karlis, T., Polemis, D., Girgin, S. C., & Syntychaki, A. (2019). Future challenges of
Maritime economics research. In Paper Presented at the IAME 2019 CONFERENCE.
Kou, Y., & Luo, M. (2018). Market driven ship investment decision using the
real option approach. Transportation Research Part A: Policy and Practice, 118,
714-729.
Li, J., & Parsons, M. G. (1997). Forecasting tanker freight rate using neural networks.
Maritime Policy & Management, 24(1), 9-30.
Lyridis, D., Zacharioudakis, P., Mitrou, P., & Mylonas, A. (2004). Forecasting tanker
market using artificial neural networks. Maritime Economics & Logistics, 6(2),
93-108.
Makridakis, S., Wheelwright, S. C., & Hyndman, R. J. (2008). Forecasting methods and
applications. John wiley & sons.
Marcus, H., Glucksman, M., Ziogas, B., & Meyer, K. (1991). A buy-low, sell-high
investment methodology: The case of bulk shipping. Interfaces, 21(2), 8-21.
Merikas, A. G., Merika, A. A., & Koutroubousis, G. (2008). Modelling the invest-
ment decision of the entrepreneur in the tanker sector: Choosing between a
second-hand vessel and a newly built one. Maritime Policy & Management, 35(5),
433-447.
Pankratz, A. (1983). Forecasting with univariate box-jenkins models: Concepts and
cases. USA: John Wily & Sons. Inc.
Pesaran, M. H., & Shin, Y. (1998). An autoregressive distributed-lag mod-
elling approach to cointegration analysis. Econometric Society Monographs, 31,
371-413.
Petropoulos, F., Kourentzes, N., Nikolopoulos, K., & Siemsen, E. (2018). Judgmental
selection of forecasting models. Journal of Operations Management, 60, 34-46.
Rasouli, S., Tabesh, H., & Etminani, K. (2016). A study of input variable selection to
artificial neural network for predicting hospital inpatient flows. Current Journal
of Applied Science and Technology, 1-8.
Rau, P., & Spinler, S. (2016). Investment into container shipping capacity: A real
options approach in oligopolistic competition. Transportation Research Part E:
Logistics and Transportation Review, 93, 130-147.
Sims, C. A. (1980). Macroeconomics and reality. Econometrica: Journal of the Econo-
metric Society, 1-48.
Stopford, M. (2009). Maritime economics 3e. Routledge.
Thalassinos, I., Hanias, M. P., Curtis, G., & Thalassinos, E. (2013). Forecasting financial
indices: The Baltic dry indices. In Marine navigation and safety of sea transporta-
tion: STCW, maritime education and training (MET), human resources and crew
manning, maritime policy, logistics and economic matters. pp. 190-283.
Tobin, J. (1969). A general equilibrium approach to monetary theory. Journal of
Money, Credit and Banking, 1(1), 15-29.
Tsolakis, S., Cridland, C., & Haralambides, H. (2003). Econometric modelling of
second-hand ship prices. Maritime Economics & Logistics, 5(4), 347-377. https://
doi.org/10.1057/palgrave.mel.9100086
Uyar, K., &I˙lhan, A. (2016). Long term dry cargo freight rates forecasting by using
recurrent fuzzy neural networks. Procedia Computer Science, 102, 642-647.
Zeng, Q., Qu, C., Ng, A. K., & Zhao, X. (2016). A new approach for Baltic Dry Index fore-
casting based on empirical mode decomposition and neural networks. Maritime
Economics & Logistics, 18(2), 192-210.
10
View publication stats