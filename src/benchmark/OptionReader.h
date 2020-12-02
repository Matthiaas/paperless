#include <iostream>
#include "../PaperlessKV.h"

using Options = PaperlessKV::Options;

std::map<std::string, std::function<Options&(Options*, size_t)>> size_t_options = {
    {"MAX_LOCAL_MEMTABLE_SIZE", &Options::MaxLocalMemTableSize},
    {"MAX_REMOTE_MEMTABLE_SIZE", &Options::MaxRemoteMemTableSize},
    {"MAX_LOCAL_CACHE_SIZE", &Options::MaxLocalCacheSize},
    {"MAX_REMOTE_CACHE_SIZE", &Options::MaxRemoteCacheSize},
    {"DISPATCH_IN_CHUNKS", &Options::DispatchInChunks},
};

std::map<std::string, std::function<Options&(Options*, std::string)>> string_options = {
    {"STORAGE_LOCATION", &Options::StorageLocation},
};

std::optional<std::string>GetEnv( const std::string & var ) {
  const char * val = std::getenv( var.c_str() );
  if ( val == nullptr ) { // invalid to assign nullptr to std::string
    return std::nullopt;
  }
  else {
    return std::string(val);
  }
}

Options ReadOptionsFromEnvVariables() {
  PaperlessKV::Options options;
  for(auto& [env, set_option] : size_t_options) {
    auto val = GetEnv(env);
    if(val) {
      set_option(&options, std::stoul(val.value()));
    }
  }
  for(auto& [env, set_option] : string_options) {
    auto val = GetEnv(env);
    if(val) {
      set_option(&options, val.value());
    }
  }
  return options;
}