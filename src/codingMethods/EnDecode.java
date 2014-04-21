/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.ac.ncic.mastiff.io.coding;

import cn.ac.ncic.mastiff.io.coding.Compression.Algorithm;
import cn.ac.ncic.mastiff.io.coding.Encoder.CodingType;

/**
 * Factory to create a <i>Encoder</i> or <i>Decoder</i>.
 * 
 * @see Encoder
 * @see Decoder
 */
public class EnDecode {

  public static Decoder getDecoder(int sortedCol_, int valueLen_, 
      Algorithm algo_, CodingType type_) {
    Decoder decompressor = null;
    if (type_ == CodingType.MV) {
      decompressor = new MVDecoder(sortedCol_, valueLen_, algo_);
    } else if (type_ == CodingType.RunLengthEncodingInt){
      decompressor = new RunLengthEncodingIntReader(sortedCol_, valueLen_,algo_);

    } else if (type_ == CodingType.RunLengthEncodingByte){
      decompressor = new RunLengthEncodingByteReader(sortedCol_, valueLen_,algo_);

    } else if (type_ == CodingType.RunLengthEncodingLong){
      decompressor = new RunLengthEncodingLongReader(sortedCol_, valueLen_,algo_);

    } else if (type_ == CodingType.DictionaryBitPackingRLEByte){
      decompressor = new DictionaryBitPackingRLEByteReader(sortedCol_, valueLen_,algo_);

    } else if (type_ == CodingType.DictionaryBitPackingRLEInt){
      decompressor = new DictionaryBitPackingRLEIntReader(sortedCol_, valueLen_,algo_);

    } else if (type_ == CodingType.DictionaryBitPackingRLELong){

      decompressor = new DictionaryBitPackingRLELongReader(sortedCol_, valueLen_,algo_);

    } else if (type_ == CodingType.DeltaBinaryArrayZigZarByte){

      decompressor = new DeltaBinaryArrayZigZarByteReader(sortedCol_, valueLen_,algo_);

    } else if (type_ == CodingType.DeltaBinaryBitPackingZigZarInt){

      decompressor = new DeltaBinaryBitPackingZigZarIntReader(sortedCol_, valueLen_, algo_);

    } else if (type_ == CodingType.DeltaBinaryBitPackingZigZarLong){

      decompressor = new DeltaBinaryBitPackingZigZarLongReader(sortedCol_, valueLen_,algo_);

    }
   else if (type_ == CodingType.DeltaBinaryPackingString){

    decompressor = new DeltaBinaryPackingStringReader(sortedCol_, valueLen_,algo_);

  }
 else if (type_ == CodingType.RedBlackTreeString){

  decompressor = new RedBlackTreeStringReader(sortedCol_, valueLen_,algo_);

}
    
    else {
      throw new UnsupportedOperationException("Unsupported codingtype " + type_);
    }
    return decompressor;
  }

  public static Encoder getEncoder(int pageCapacity_, int valueLen_, int startPos_, 
      Algorithm algo_,Encoder.CodingType type_) {
    Encoder compressor;
    if (valueLen_ > 0) {
      switch (type_){
      case   MV :
        // System.out.println("86!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!new FixedLenEncoder"+"  pageCapacity_    "+pageCapacity_);
        compressor = new FixedLenEncoder(pageCapacity_, valueLen_, startPos_, algo_);   break ;
      case  RunLengthEncodingInt :
        compressor=new  RunLengthEncodingIntWriter(pageCapacity_, valueLen_, startPos_, algo_);   break ; 
      case  RunLengthEncodingByte :
        compressor=new  RunLengthEncodingByteWriter(pageCapacity_, valueLen_, startPos_, algo_);   break ;  
      case  RunLengthEncodingLong :
        compressor=new  RunLengthEncodingLongWriter(pageCapacity_, valueLen_, startPos_, algo_);   break ;  
      case  DeltaBinaryBitPackingZigZarInt :
        compressor=new  DeltaBinaryBitPackingZigZarIntWriter(pageCapacity_, valueLen_, startPos_, algo_);   break ;  
      case  DeltaBinaryBitPackingZigZarLong :
        compressor=new  DeltaBinaryBitPackingZigZarLongWriter(pageCapacity_, valueLen_, startPos_, algo_);   break ;  
      case  DeltaBinaryArrayZigZarByte :
        compressor=new  DeltaBinaryArrayZigZarByteWriter(pageCapacity_, valueLen_, startPos_, algo_);   break ;  
      case  DictionaryBitPackingRLEInt :
        compressor=new  DictionaryBitPackingRLEIntWriter(pageCapacity_, valueLen_, startPos_, algo_);   break ;  
      case  DictionaryBitPackingRLEByte :
        compressor=new  DictionaryBitPackingRLEByteWriter(pageCapacity_, valueLen_, startPos_, algo_);   break ;  
      case  DictionaryBitPackingRLELong :
        compressor=new  DictionaryBitPackingRLELongWriter(pageCapacity_, valueLen_, startPos_, algo_);   break ;  

      default :  throw new UnsupportedOperationException("Unsupported codingtype " + type_); 
      }
    } else {
      switch (type_){
      case  DeltaBinaryPackingString :
        System.out.println("106 ......................  new DeltaBinaryPackingString   "+"  pageCapacity_  "+pageCapacity_+"   startPos_  "+startPos_+"   algo_ "+algo_);
        compressor=new DeltaBinaryPackingStringWriter(pageCapacity_, valueLen_, startPos_, algo_);   break ;  
      case  RedBlackTreeString:
         compressor=new  RedBlackTreeStringWriter(pageCapacity_, valueLen_, startPos_, algo_);   break ;
      case MV:
        System.out.println("106 ......................  new VarLenEncoder   "+"  pageCapacity_  "+pageCapacity_+"   startPos_  "+startPos_+"   algo_ "+algo_);
        compressor = new VarLenEncoder(pageCapacity_, valueLen_, startPos_, algo_);break ;
         default :throw new UnsupportedOperationException("Unsupported codingtype " + type_); 
           
      }
 //     System.out.println("106 ......................  new VarLenEncoder   "+"  pageCapacity_  "+pageCapacity_+"   startPos_  "+startPos_+"   algo_ "+algo_);
  //    compressor = new VarLenEncoder(pageCapacity_, startPos_, algo_);
    }
    return compressor;
  }

}
