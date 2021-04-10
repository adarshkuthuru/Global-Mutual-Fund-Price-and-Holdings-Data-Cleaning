library(plyr)


#Holdings data
# get all the zip files
zipF <- list.files(path = "E:/Drive/Morningstar data/Global equity funds data/Raw Data Received from Girjinder/Portfolio/All files", pattern = "*.zip", full.names = TRUE)

outDir<-"E:/Drive/Morningstar data/Global equity funds data/Processed Data/Portfolio"
#unzip(zipF,exdir=outDir)

# unzip all your files
ldply(.data = zipF, .fun = unzip, exdir = outDir)

###Price data
# get all the zip files
zipF <- list.files(path = "E:/Drive/Morningstar data/Global equity funds data/Raw Data Received from Girjinder/Price_29122017", pattern = "*.zip", full.names = TRUE)

outDir<-"E:/Drive/Morningstar data/Global equity funds data/Processed Data/Price"
#unzip(zipF,exdir=outDir)

# unzip all your files
ldply(.data = zipF, .fun = unzip, exdir = outDir)
