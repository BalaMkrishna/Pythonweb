from .News.facebookScrap import storeToAztableFacebook
from .News.twitter import storeToAzuretableTwitterNews
from .News.newscrap import storeToAzuretableAticleNews
from .News.Instagram import storeToAzuretableInstagramNews
from .News.linkedin import store_in_azure_linkedIn

from .ProductFeatures.Feature_Afas import featuresafas
from .ProductFeatures.Features_ebookhouden import featureseboekhouden
from .ProductFeatures.features_Exact import featuresexact
from .ProductFeatures.Features_twinfield import featurestwinfield
from .ProductFeatures.Features_visma import featuresvisma
from .ProductFeatures.Features_silvasoft import featuressilvasoft
from .ProductFeatures.Features_snelstart import featuressnelstart
from .ProductFeatures.Feature_Jortt import featuresjortt
from .ProductFeatures.Features_yuki import featuresyuki
from .ProductFeatures.Features_informer import featuresinformer

from .ProductInfo.Afas import storeToAztable_Afas
from .ProductInfo.ebookhouden import storeToAztable_eboekhouden
from .ProductInfo.ExactWeb import storeToAztable_ExactWeb
from .ProductInfo.Twinfield import storeToAztable_Twinfield
from .ProductInfo.silvasoft import storeToAztable_silvasoft
from .ProductInfo.snelstart import storeToAztable_snelstart
from .ProductInfo.jortt import storeToAztable_jortt
from .ProductInfo.yuki import storeToAztable_yuki
from .ProductInfo.informer import storeToAztable_informer
from .ProductInfo.visma import storeToAztable_visma

from .Reviews.review_appwiki import storeToAztable_reviewAppwiki
from .Reviews.review_google import storeToAztable_reviewGoogle
from .Reviews.review_trustpilot import storeToAztable_reviewTrustpilot
from .Reviews.review_appstore import appstorereviews
from .Reviews.review_playstore import playstorereviews



def runner():
    storeToAztableFacebook()
    storeToAzuretableAticleNews()
    storeToAzuretableInstagramNews()
    store_in_azure_linkedIn()
    storeToAzuretableTwitterNews()
    featuresafas()
    featureseboekhouden()
    featuresexact()
    featurestwinfield()
    featuresvisma()
    featuressilvasoft()
    featuressnelstart()
    featuresjortt()
    featuresyuki()
    featuresinformer()
    storeToAztable_Afas()
    storeToAztable_eboekhouden()
    storeToAztable_ExactWeb()
    storeToAztable_Twinfield()
    storeToAztable_silvasoft()
    storeToAztable_snelstart()
    storeToAztable_jortt()
    storeToAztable_yuki()
    storeToAztable_informer()
    storeToAztable_visma()
    storeToAztable_reviewAppwiki()
    storeToAztable_reviewGoogle()
    storeToAztable_reviewTrustpilot()
    appstorereviews()
    playstorereviews()

    return 0
