use std;

#[macro_export]
macro_rules! serial_writer {
    ( $( $x:expr ),* ) => {
        {
            let mut out = Vector::new();           
            $(
                temp_vec.push($x);
            )*
            temp_vec
        }
    };
}

mod messages{

  //  SerialWriter(const Fields&... fields){
        //         (fieldSize(fields), ...);
        
        //         outSize_ += intSz_;
        //         out_.resize(outSize_);
        //         writeField(outSize_);
        
        //         (writeField(fields), ...);
        //     } 

    

}
// template<typename... Fields>
// class SerialReader{
// public: 
//     SerialReader(const std::string& m, Fields&... fields):
//         in_(m){        
//         const int allType = intSz_;
//         inSize_ = int(m.size());         
//         char* pData = (char*)m.data();
//         if (inSize_ < allType || inSize_ != *(int*)(pData)){
//             ok_ = false;
//             return;
//         }
//         offs_ = allType;
//         (readField(fields), ...);
//     } 
//     bool ok()const{
//         return ok_;
//     }
// private:
//     bool checkFieldSize(int fieldSize){
//         if (offs_ + fieldSize > inSize_){
//             ok_ = false;
//         }
//         return ok_;        
//     }
//     void readField(std::string& s){ 
//         if (ok_){
//             if (!checkFieldSize(intSz_)) return;
//             const char* pData = in_.data();
//             int strSz = *((int*)(pData + offs_));  offs_ += intSz_;
            
//             if (!checkFieldSize(strSz)) return;
//             s = std::string(pData + offs_, strSz);  offs_ += strSz;
//         }        
//     }
//     void readField(int& v){
//         if (ok_){
//             if (!checkFieldSize(intSz_)) return;
//             const char* pData = in_.data();
//             v = *((int*)(pData + offs_));  offs_ += intSz_;
//         }
//     }
//     void readField(std::vector<int>& out){
//         if (ok_){
//             if (!checkFieldSize(intSz_)) return;
//             const char* pData = in_.data();
//             int vsz = *((int*)(pData + offs_));  offs_ += intSz_;
//             if (!checkFieldSize(vsz * intSz_)) return;
//             out.reserve(vsz);
//             memcpy(out.data(), pData + offs_, vsz * intSz_);
//             offs_ += vsz * intSz_;
//         }
//     }
//     const int intSz_ = 4;
//     int inSize_{};    
//     int offs_ = 0;
//     bool ok_ = true;
//     const std::string& in_;
// };

// template<typename... Fields>
// class SerialWriter{
// public: 
//     SerialWriter(const Fields&... fields){
//         (fieldSize(fields), ...);

//         outSize_ += intSz_;
//         out_.resize(outSize_);
//         writeField(outSize_);

//         (writeField(fields), ...);
//     } 
//     std::string out(){
//         return out_;
//     }
// private:
//     void fieldSize(const std::string& s){
//         outSize_ += intSz_ + s.size();
//     }
//     void fieldSize(int){
//         outSize_ += intSz_;
//     }
//     void fieldSize(const std::vector<int>& v){
//         outSize_ += intSz_ + int(v.size()) * intSz_;
//     }
//     void writeField(const std::string& s){ 
//         char* pOut = out_.data();
//         const auto ssz = s.size();
//         *((int*)(pOut + offs_)) = ssz;        offs_ += intSz_;
//         memcpy(pOut + offs_, s.data(), ssz);  offs_ += ssz;
//     }
//     void writeField(int v){
//         char* pOut = out_.data();
//         *((int*)(pOut + offs_)) = v; offs_ += intSz_;
//     }
//         void writeField(const std::vector<int>& arr){
//         char* pOut = out_.data();
//         const int asz = int(arr.size());
//         *((int*)(pOut + offs_)) = asz; offs_ += intSz_;
//         memcpy(pOut + offs_, arr.data(), asz * intSz_);
//         offs_ += asz * intSz_;
//     }
//     const int intSz_ = 4;
//     int outSize_{};    
//     int offs_ = 0;
//     std::string out_;        
// };
