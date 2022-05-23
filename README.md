# data-extractor-rs

### DESIGN
<p>Event-Driven Data Warehouse Creation for NLP and on-chain analytics, with a focus on NFT data. </br></br>
A 'flow' is an ETL pipeline sequence that generates an output, in the form of either transformed data or analysis. </br>
A 'flow_step' signifies an operation on set of data. (copy, move, transform, ...) </br>
The modular design allows for scalable, custom ETL pipeline creation by chaining together relevant processes. </br>
</p>

<p align="center" width="15%" size="50%">
   <img src="work/db_design_flows.png">  
</p>

### CLI
