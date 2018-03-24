{-# LANGUAGE ScopedTypeVariables #-}
module Data.Avro.JSON where

import Control.Monad ((>=>))

import Data.Semigroup ((<>))

import qualified Data.Aeson as Aeson
import Data.ByteString.Lazy (ByteString)
import Data.Tagged

import Data.Avro (FromAvro (..), ToAvro (..), Result (..), parseAvroJSON)
import qualified Data.Avro as Avro
import Data.Avro.Schema (Schema)
import qualified Data.Avro.Schema as Schema

fromJSON :: forall a. (FromAvro a) => Aeson.Value -> Result a
fromJSON json = parseAvroJSON env schema json >>= fromAvro
  where schema = untag (Avro.schema :: Tagged a Schema)
        env = Schema.buildTypeEnvironment missing schema . Schema.TN
        missing name = fail $ "Type " <> show name <> " not in schema."

parseJSON :: forall a. (FromAvro a) => ByteString -> Result a
parseJSON input = case Aeson.eitherDecode input of
  Left msg    -> Error msg
  Right value -> fromJSON value

toJSON :: forall a. (ToAvro a) => a -> Aeson.Value
toJSON = Aeson.toJSON . toAvro
