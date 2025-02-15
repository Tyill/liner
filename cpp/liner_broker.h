#pragma once

#include "../include/liner.h"

#include <string>
#include <functional>

class LinerBroker {

public:
    
  LinerBroker(const std::string& clientName, const std::string& topic, const std::string& addr, const std::string& redisAddr){
      m_hClient = lnr_new_client(clientName.c_str(), topic.c_str(), addr.c_str(), redisAddr.c_str());
  }
  ~LinerBroker(){
      if (m_hClient){
          lnr_delete_client(m_hClient);
      }
  }
  using ReceiveCBack = std::function<void(const std::string& to, const std::string& from, const std::string& data)>;
  
  bool run(ReceiveCBack cb){
      if (m_hClient){
          m_receiveCBack = cb;
          return lnr_run(m_hClient, receiveCb, this);
      }
      return false;
  }

  bool sendTo(const std::string& topic, const std::string& data){
      if (m_hClient){
          return lnr_send_to(m_hClient, topic.c_str(), data.data(), data.size(), TRUE);
      }
      return false;
  }

  bool sendAll(const std::string& topic, const std::string& data){
      if (m_hClient){
          return lnr_send_all(m_hClient, topic.c_str(), data.data(), data.size(), TRUE);
      }
      return false;
  }

  bool subscribe(const std::string& topic){
      if (m_hClient){
          return lnr_subscribe(m_hClient, topic.c_str());
      }
      return false;
  }

  bool unsubscribe(const std::string& topic){
      if (m_hClient){
          return lnr_unsubscribe(m_hClient, topic.c_str());
      }
      return false;
  }

  bool clear_addresses_of_topic(){
      if (m_hClient){
          return lnr_clear_addresses_of_topic(m_hClient);
      }
      return false;
  }

  bool clear_stored_messages(){
      if (m_hClient){
          return lnr_clear_stored_messages(m_hClient);
      }
      return false;
  }

  private:
    static void receiveCb(const char* to, const char* from,  const char* data, size_t data_size, void* udata){
        if (udata){
            auto clt = static_cast<LinerBroker*>(udata);
            if (clt->m_receiveCBack){
                clt->m_receiveCBack(to, from, std::string(data, data_size));
            }
        }   
    }
    lnr_hClient m_hClient{};
    ReceiveCBack m_receiveCBack{};

};