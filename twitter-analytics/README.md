# Labor market analysis with Twitter data


Online social networks, such as Twitter, play a key role in the diffusion of information on jobs. For instance, companies and job aggregators post job offers while users disclose their labor market situation seeking emotional support or new job opportunities through their online network. In this context, Twitter data can be seen as a complementary data source to official statistics as it provides timely information about labor market trends.

In this project, we leverage state-of-the-art language models (Devlin et al, 2018) to accurately identify disclosures on personal labor market situations as well as job offers. The methodology is presented in this [IC2S2 2020 presentation](https://www.youtube.com/watch?v=ZxFrtUW2dYA) and detailed in Tonneau et al. (2021) (in review). Aggregating this individual information at the city and country levels, we then built Twitter-based labor market indexes and used them to better predict future labor market trends. 


In this folder, we provide resources to reproduce our methodology, in order to identify disclosures of labor market situations on the one hand, as well as build labor market indexes using this individual information on the other hand. The structure of the folder is as follows:

- The content of the book's chapter on Twitter analytics is stored in `bookdown`.
- The source code is stored in `code`. The latter contains:
  - `1-training_data_preparation`: define random sets (one to evaluate the models and one to sample new tweets from), build surveys in Qualtrics and send them to MTurk to have tweets labelled
  - `2-model_training`: finetune BERT-based model for job disclosure and job offer classification. A detailed README is included.
  - `3-model_evaluation`: resources to evaluate models in terms of precision, recall/expansion and diversity
  - `4-inference_200M`: convert models to ONNX and run inference on two random sets
  - `5-active_learning`: apply active learning methodologies to sample new tweets to label and feed to the model. A detailed README is included. 
  - `6-deployment`: deploy classifiers on the entire tweet dataset.  A detailed README is included. 
- The code to produce indicators and the output data is stored in `indicators`. A detailed README is included. 

Additionally, we provide [a basic example](https://github.com/worldbank/TwitterEconomicMonitoring/blob/master/notebooks/4-build-unemployment-index.ipynb) using a keyword-based approach to build a Twitter-based unemployment index for Mexico. 

## References:

Devlin, Jacob, et al. "Bert: Pre-training of deep bidirectional transformers for language understanding." arXiv preprint arXiv:1810.04805 (2018).

Tonneau, Manuel, et al. "BERT is Looking for a Job: Identifying Unemployment on Twitter using BERT and Active Learning" (in review)
