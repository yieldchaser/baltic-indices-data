# Maritime Data delivered via Signal’s APIs

**Date**: March 15, 2022 | **Category**: Market Insights | **Section**: Newsroom | **Source**: [https://www.thesignalgroup.com/newsroom/maritime-data-api-and-sdk](https://www.thesignalgroup.com/newsroom/maritime-data-api-and-sdk)

---

The last few years have seen significant developments in the area of shipping technology. Buyers have broadened and matured their requirements for technology, while suppliers have grown in numbers like never before. The areas of interest are diverse, aimed at commercial, technical, and operational impact, and very often are driven by the very-much-in-focus ESG (Environment, Social, and Government) agendas the industry is now pursuing including tanker chartering. A natural by-product of a maturing landscape is the greater availability of data, although the lack of standardisation continues to be a significant obstacle, standing in the way of credible, effective technology applications.Signal Ocean has developed a comprehensive list of Application Programming Interfaces (APIs)that allow their users to embed state-of-the-art analysis algorithms, carefully curated industry data, and their own proprietary information into their real-time market analytics. Our unique data fusion technology allows our customers to maintain their hard-to-come-by information advantages, avoid getting into another “war for the desktop” discussion, and save on unnecessary sizable and risky investments.

Before diving into details, let’s start with the basics.


## What is Maritime Data, and why use anAPIor SDK?

Maritime Data obviously pertains to and describes the seaborne transportation domain. Vessel-related data are key, data such as vessel name and/or other global identifiers (IMO numbers, call signs, etc) alongside a rich set of data on vessel specifications/characteristics/particulars. Historical positioning data, speeds, headings, coordinates follow suit coming primarily from the Automatic Identification System (AIS). Maritime datasetsalso include a wide spectrum of operational and commercial data, such as ship destinations or stops, loading/discharging operations, cargo on board, quantities, emissions, commercial operators, charterers, freight rates, and much more. A critical component of this data is the maritime AIS system, which provides real-time information about vessel positions, movements, and status, essential for navigation safety, maritime security, and environmental protection.

Moving beyond data content, and onto the data delivery methods, APIs are very popular among data scientists, market/data analysts, and professionals that need to collect, sort, ingest and combine significant amounts of data. Making the most of Signal’s APIs allows users to leverage that data for any purpose combining datasets from different data sources and building private analytics reports that empower their decision making.From performing ad-hoc analysis, building a data lake, or integrating with your own or third-party systems like PowerBI or Tableau, ourAPIsare optimised for simplicity, facilitating seamless integration and data flow.


## AIS data - why does it matter? What types of AIS data do we source and use inSignal Ocean?

At Signal we source AIS data from a variety of terrestrial and satellite sources. Not all AIS feeds are equal in coverage and quality. We have worked hard to curate the right sources, but more importantly to use and combine them in a way that elevates quality and extends coverage. Our integration includes visual representation on marine maps, providing an intuitive way to monitor vessel movements globally.

This means that when there are potential disruptions like what recently happened in the Panama or Suez canals or similar, we are well positioned to overcome and offer a best-of-breed solution to our customers.

AIS data used in The Signal Ocean Platform constitute copyright and intellectual property rights protected material of various sources including Vesseltracker GmbH, Marine Traffic and Spire Global.


## How has Maritime Data evolved?

Technology generally stops for no one, and the shipping industry has no choice but to adapt. The change has been partly driven by the wide availability of AIS data now widely available with multiple AIS providers offering terrestrial and satellite coverage globally. However, as it has been the case in every other industry touched by big data, the maritime data landscape is ripe for change.

The change has been partly driven by the wide availability of AIS data, no longer a well-kept secret but now widely available with multiple AIS providers offering terrestrial and satellite coverage globally. On top of that, using state-of-the-art capabilities from the domain of Artificial Intelligence, Signal allows its users to have  “unstructured” private commercial data automatically detected, extracted and processed on their behalf. This data is then combined, or 'fused,' with data Signal has been carefully curating, extending and keeping up-to-date for years to answer market situational awareness questions in real-time, showcasing our powerful data integration capabilities.

The true potential of such deeply analytical approaches in shipping is starting to be evident and one can draw analogies from the effect of big data on adjacent domains to understand what this could mean for the maritime industry going forward. What is certainly interesting is that many if not most market participants have high volumes of data that remain idle, unconnected and largely untapped. Organising it into productive insights can truly be a game-changer for those who will go after the opportunity.


## Signal Ocean APIs


## Voyages Data API

With Voyages Data API you can develop insights from where all ships on water have been (since 2014) and will be going, based on our patented forecasting algorithm, fused with a wealth of commercial and operational voyage details. Some questions that the user can answer:

- The number of cargoes that have loaded at any port, country, area around the world
- Forecast the number of vessels expected to discharge in the near future at any port, country, area around the world
- Monitor and demystify Commodity flows from all origins to all destinations.
- Generate a matrix of all load, discharge combinations for any country and assess how global trade has changed over time
- Analyse the key players, charterers or commercial operators for different trading geographies
- Investigate port congestions for any port around the world. Assess waiting times for load or discharge operations
- Analyse variations in Ton days and Ton miles metrics, allowing users to assess live changes in freight demand across specific geographies
- Benchmark market players against each other in order to assess utilisations levels, triangulation & average waiting times
- Combine vessels trading patterns with relevant Fixture data


![An indicative dashboard created by Voyages API data](../images/66eab29a53002021b08e8469_66eaafc7b96895f8d810d5b9_Screenshot%202024-09-18%20at%2011.47.15.avif)
*An indicative dashboard created by Voyages API data*


## Daily Vessel State API

The Daily Vessel State API provides historical data and real-time updates on the status of vessels, offering critical insights for shipping markets. With this API, users can perform:

- Market State Analysis: Monitor historical trading trends correlated with shipping indices to access the current state of the market and create signals for future movements.
- Operational Monitoring: Track the current status and activities of vessels to manage and optimise fleet operations.
- Voyage Analysis: Analyse daily movements and operational states of vessels for enhanced voyage planning and execution.
- Compliance Tracking: Monitor vessels for compliance with maritime regulations and standards.
- Incident Response: Quickly identify and respond to incidents or anomalies in vessel operations.


![Signal Figure](../images/66eab29a53002021b08e846c_66eab0dae06295ef0b06c99e_Screenshot%202024-09-18%20at%2011.50.26.avif)
*Signal Figure*


## Tonnage list API

With the Signal Tonnage List API you can monitor how actual vessel supply has evolved for any route and trade (since 2015). You can get vessel availability information for any port in the world, both live and point in time. Tonnage availability is a leading supply indicator with a direct impact on freight rates. Some questions that the user can answer:

- The number of commercially available vessels that can reach any load port in the world at any future window, eg. the next 10 days, the next 20 days, the next month, point in time going back to 2015.
- Run analytics around vessels’ market deployment. Eg. How the number of spot and relet vessels has changed over time in a certain geographical region
- Number of vessels on subjects or failed on a daily basis
- Correlate vessel supply with key spot market rates


![Signal Figure](../images/66b51de8de38288155690503_622b5a33e6631a69db362cfa_Tonnage-List-API.avif)
*Signal Figure*


## Scraped data API

Market participants are bombarded on a daily basis with thousands of emails containing valuable commercial data which are almost impossible to process without the help of technology.

Data extraction and comprehension technologies have moved leaps and bounds over the last couple of years and we are harnessing this to help our customers save time and take advantage of this otherwise sub-optimally used treasure trove of information. Our algorithms will detect and successfully process more than 90% of the relevant information that is buried in your mailboxes, before of course cleaning it up and making it available for use in analysis and forecasting.All extracted data are of course private to your account and are not exposed to any other Signal customer..

Some questions that the user can answer:

- The number of cargoes being quoted on a daily basis, real-time
- The fixtures reported in their market with their corresponding last done levels
- The open positions currently advertised looking for their next employment
- The lineups announcing upcoming arrivals and departures of vessels including berth dates, next destination and others


![Signal Figure](../images/66b51de8de382881556904fd_622b5a485b53ba240f26c35e_Scraped-Data-API.avif)
*Signal Figure*


## Vessel Emissions API

Withmaritime decarbonisationbeing in the front line of the industry, calculating and analysing vessel emissions and ratings becomes increasingly important. The Vessel Emissions API allows tracking key emissions (CO2, NOx, SOx, CH4, among others) per vessel, voyage, ballast/laden leg, and vessel operation, and calculate efficiency metrics, like EEOI, AER. The API also provides insights into the alignment and rank of vessels against their peers based on industry standards (IMO, Poseidon Principles, Sea Cargo Charterer). The Vessel Emissions API could provide insights on the following:

- Produce emission reports for your fleet’s voyages, split by Ballast, Laden, Port call, and Stop voyage segments
- Compare fleet emissions across different Commercial Operators, in terms of normalised [gram / ton-mile] and Total CO2 emissions, or any other efficiency metrics, aggregated per calendar year.
- Compare the CO2 emissions for the top Commercial Operators in vessel classes of interest.
- Analyse EEOI, AER, CII, alignment scores, and rank vessels against the industry standards (IMO, Poseidon Principles, Sea Cargo Charterer)


![An indicative dashboard created by Vessel Emissions API data](../images/66eab29a53002021b08e8456_66eab135c22988d55d7ac897_Screenshot%202024-09-18%20at%2011.53.27.avif)
*An indicative dashboard created by Vessel Emissions API data*

‍


## Vessels API

The vessels API is a core service covering more than 40,000 vessels across tankers, dry bulk, containers, LPG and LNG. It includes vessel particulars like dimensions and capacity, Shipyard built, as well as more complex characteristics like scrubber fitting, ballast water treatment installation and special equipment on board.

With the Vessels API you can find the answer to the following questions:

- Run analytics around the growth of the global fleet
- Assess percentage of scrubber fitted vessels across all fleets
- Understand vessels’ commercial management information over time
- Analyse the global fleet age and deadweight profile

‍


![Signal Figure](../images/66b51de8de382881556904fa_622b6f89e6ce8b1b34ba5d89_Vessels-API.avif)
*Signal Figure*

‍


## Vessel Distances API

Accurate routes and sea distance calculation are critical in taking the right decisions. By using our Vessel Distances API you integrate the fastest distance calculation engine into your own systems, capable of running port-to-port, point-to-port and vessel-to-port distances.

- Calculate a distance to any ports, taking into account Sulfur Emission Control Areas (SECA), piracy zones, canals restrictions
- Create a distance matrix of origins and destinations
- Run speed sensitivities for any port combinations


![Signal Figure](../images/66b51de8de38288155690509_622b5a6e86571a2cf2ddef9a_Distance-API.avif)
*Signal Figure*

‍


## Market Rates API

The Market Rates API offers access to historical and current market rates across key trading routes for various tanker markets. This API enables users to:

- Market Analysis: Identify trends and forecast future market conditions by analysing historical and current market rates.
- Rate Comparison: Compare rates across different trading routes and tanker markets to find the most profitable options.
- Cost Estimation: Estimate transportation costs, aiding in budgeting and financial planning.
- Strategic Planning: Develop strategic plans for fleet deployment and chartering decisions based on comprehensive market data.


![Signal Figure](../images/66eab29a53002021b08e846f_66eab1b2acbe8b531f3d7cde_Screenshot%202024-09-18%20at%2011.55.15.avif)
*Signal Figure*


## Geos API

‍

The Geos API provides detailed geographical data, including an expanded list of ports, locations for all maritime regions. This API enhances operational planning and navigation by offering:

- Operational Planning: Improved planning accuracy with detailed geographical data, including ports and terminals.
- Route Optimisation: Optimise navigation routes with comprehensive geographical information, including SECA zones and other regulatory areas.
- Search Efficiency: Accelerate searches with intuitive display names and widely used abbreviations, making it easier to find relevant locations quickly.

‍


## Freight API

Price discovery in shipping is infamously hard, especially for outsiders. Of course, assessing freight prices is bread and butter for most spot market participants. Some routes are easy to assess and the latest market levels are not hard to come by. Other trades are harder to assess with low liquidity and infrequent fixing.

With a wealth of data on recent fixtures, reported market assessments, port costs, distances and much more, Signal’s advanced pricing algorithms can help you assess freight cost like never before. With the Freight API you can get time series data on the evolution of freight pricing ($/ton) and estimate flat rates for the richest combinations of ports across all large tanker segments and reply to some of the following questions:

- Retrieve the freight cost ($/ton) of cargo, between the load and discharge ports, across different vessel sizes
- Convert WS rates to $/ton costs allowing traders and charterers to easier analyse transportation costs
- Include canal costs in the total freight assessment
- Assess freight levels historically going back to 2018


![Signal Figure](../images/66b51de9de38288155690667_622b5a7e2118be0dfb804121_Freight-API.avif)
*Signal Figure*

‍


## Vessel Valuation API

The Vessel Valuations API helps you estimate the current and historical price of any vessel. Valuations are calculated through a multi-factor pricing model that is based on market sentiment, key valuation criteria (i.e. type, size, age), and fine-tunes the estimations based on the specific characteristics of each vessel, as required. With theVessel ValuationAPI you can get insights on the following:

- Retrieve current and historical valuation and demolition price estimates for one or more vessels
- Retrieve and analyse how the valuation would have evolved given a vessel’s current age (constant age historical time series)
- Run analytics to get a deep understanding of the evolution of market values per vessel size and age group


![Signal Figure](../images/66b51de8de38288155690500_622b6ff01ff829c0bd5023be_Vessel-Valuations-API.avif)
*Signal Figure*


## Port Expenses API

Port expenses are an important and difficult to track financial aspect of any commercial voyage, from the planning phase all the way to booking and accounting. These expenses vary between the ports and may depend on various factors such as vessel characteristics, cargo type and time of the operation. Signal provides estimates and breakdowns of these expenses for the majority of the key ports worldwide and constantly monitors for any updates. Here are a few examples on what you can achieve through the Port Expenses API:

- Retrieve the port expenses for a given port for a given different vessel
- See the breakdown of the different expense items


![Signal Figure](../images/66b51de8de38288155690506_622b766be0c5eb8050fb3cc9_Port-Expenses-API.avif)
*Signal Figure*


## Signal APIs to help startups accelerate their product growth

Using Signal APIs Startup companies can build software solutions for the shipping, supply chain and commodity trading space that rely on processing fundamental maritime data, such as details around a vessel's whereabouts, the nature or duration of its operations, theforecasting of where vesselswill be in the near future, the routes they take, the cargoes they carry, the fuel they consume and the emissions they produce, etc.

Signal's API suite opens up integration opportunities with other maritime and commodity tech companies. In this way, we are hoping to accelerate product development and minimise duplicate work across the industry. To provide a real life example, Startups likeOilXandDBX, that specialise in commodity flow analytics in the wet and dry segments respectively, have leveraged our Voyage API to build the core of their products, hence accelerating their go-to-market by more than a year.

We offer flexible deals for early-stage startups to hook up, build and commercialise new products on top of our APIs.


## Conclusion - APIs are the backbone of a maritime technology operating system

The fuel for digital innovation in the maritime industry is data. Leveraging this data can help individuals and teams identify trends, predict trading patterns and much more. But data in shipping come in all shapes and sizes, denominations, units, etc. Sometimes they are fragmented or incomplete, other times they flow over private networks and almost always they are almost impossible to align and combine.

With today’s technology, many of these issues can be addressed if not eliminated. Application Programming Interfaces (APIs) are the natural language that software applications use to talk to each other. They bring data to life, removing collaboration barriers, while at the same time providing a way to analyse and scale. Possible use cases of Signal’s APIs include fleet performance optimisation, cargo and vessel tracking fused with AIS data and commercial data, monitoring commercial availability of vessels or market rates for a given day/period and many others.

We have only just scratched the surface of what can be achieved through Signal’s APIs. Stay tuned for more as we discover more in collaboration with our partners.

If you are interested in our APIs, contact ushere.

-Republishing is allowed with an active link to the source
