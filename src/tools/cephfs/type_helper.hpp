#ifndef TYPE_HELPER_HPP__
#define TYPE_HELPER_HPP__

template<typename T1, typename T2>
T1 conv_t(T2 s){
    T1 target;
    std::stringstream conv;
    conv << s;
    conv >> target;
    return target;
}

void string_split(std::string str, vector<string>& out, string split = ":"){
    std::cout << str << std::endl;
    auto pos = str.find(split);
    while(pos != std::string::npos){
        std::cout << str.substr(0, pos) << std::endl;
        out.push_back(str.substr(0, pos));
        if (str.size() > pos + split.size()){
            str = str.substr(pos + split.size());
            pos = str.find(split);
        }else
            return;
    }
    out.push_back(str.substr());
    return;
}
#endif // TYPE_HELPER_HPP__
