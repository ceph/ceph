#include <boost/spirit/include/classic_core.hpp>
#include <iostream>
#include <string>
#include <list>
#include "s3select_oper.h"
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include <functional>

using namespace std;
using namespace BOOST_SPIRIT_CLASSIC_NS;
#define _DEBUG_TERM {string  token(a,b);std::cout << __FUNCTION__ << token << std::endl;}

/// AST builder

class s3select_projections {

    private:
        list<base_statement *> m_projections;
    
    public:
        bool is_aggregate()
        {
            //TODO iterate on projections , and search for aggregate
            //for(auto p : m_projections){}

	    return false;
        }

        bool semantic()
        {
            //TODO check aggragtion function are not nested 
            return false;
        }

        list<base_statement *> * get()
        {
            return &m_projections;
        }

};

struct actionQ
{

    list<mulldiv_operation::muldiv_t> muldivQ;
    list<addsub_operation::addsub_op_t> addsubQ;
    list<arithmetic_operand::cmp_t> arithmetic_compareQ;
    list<logical_operand::oplog_t> logical_compareQ;
    list<base_statement *> exprQ;
    list<base_statement *> funcQ;
    list<base_statement *> condQ;
    std::string from_clause;
    list<std::string> schema_columns;
    s3select_projections  projections;

};

class base_action
{

public:
    base_action() : m_action(0) {}
    actionQ *m_action;
    void set_action_q(actionQ *a) { m_action = a; }
};

struct push_from_clause : public base_action
{
    void operator()(const char *a, const char *b) const
    {
        std::string token(a, b);
        m_action->from_clause = token;
    }

} g_push_from_clause;

struct push_number : public base_action //TODO use define for defintion of actions
{
    void operator()(const char *a, const char *b) const
    {
        string token(a, b);
        variable *v = new variable(atoi(token.c_str())); //TODO strtoll

        m_action->exprQ.push_back(v);
    }

} g_push_number;

struct push_float_number : public base_action //TODO use define for defintion of actions
{

    void operator()(const char *a, const char *b) const
    {
        string token(a, b);

        //the parser for float(real_p) is accepting also integers, thus "blocking" integer acceptence and all are float.
        parse_info<> info = parse(token.c_str(), int_p, space_p);

        if (!info.full)
        {
            char *perr;
            double d = strtod(token.c_str(), &perr);
            variable *v = new variable(d);

            m_action->exprQ.push_back(v);
        }
        else
        {
            variable *v = new variable(atoi(token.c_str())); //TODO strtoll

            m_action->exprQ.push_back(v);
        }
    }

} g_push_float_number;

struct push_string : public base_action //TODO use define for defintion of actions
{

    void operator()(const char *a, const char *b) const
    {
        a++;b--;// remove double quotes
        string token(a, b);
        
        //strdup allocation should release only at the end of query-execution (class{runtime_allocator} not accessing those allocationr) 
        variable *v = new variable(strdup( token.c_str() ),variable::var_t::COL_VALUE);

        m_action->exprQ.push_back(v);
    }

} g_push_string;

struct push_variable : public base_action
{
    void operator()(const char *a, const char *b) const
    {
        string token(a, b);

        variable *v = new variable(token.c_str());

        m_action->exprQ.push_back(v);
    }
} g_push_variable;

/////////////////////////arithmetic unit  /////////////////
struct push_addsub : public base_action
{
    void operator()(const char *a, const char *b) const
    {
        string token(a, b);

        if (token.compare("+") == 0)
            m_action->addsubQ.push_back(addsub_operation::addsub_op_t::ADD);
        else
            m_action->addsubQ.push_back(addsub_operation::addsub_op_t::SUB);
    }
} g_push_addsub;

struct push_mulop : public base_action
{
    void operator()(const char *a, const char *b) const
    {
        string token(a, b);

        if (token.compare("*") == 0)
            m_action->muldivQ.push_back(mulldiv_operation::muldiv_t::MULL);
        else if (token.compare("/") == 0)
            m_action->muldivQ.push_back(mulldiv_operation::muldiv_t::DIV);
        else
            m_action->muldivQ.push_back(mulldiv_operation::muldiv_t::POW);
    }
} g_push_mulop;

struct push_addsub_binop : public base_action
{
    void operator()(const char *a, const char *b) const
    {
        base_statement *l = 0, *r = 0;

        r = m_action->exprQ.back();
        m_action->exprQ.pop_back();
        l = m_action->exprQ.back();
        m_action->exprQ.pop_back();
        addsub_operation::addsub_op_t o = m_action->addsubQ.back();
        m_action->addsubQ.pop_back();
        addsub_operation *as = new addsub_operation(l, o, r);
        m_action->exprQ.push_back(as);
    }
} g_push_addsub_binop;

struct push_mulldiv_binop : public base_action
{
    void operator()(const char *a, const char *b) const
    {
        base_statement *vl = 0, *vr = 0;

        vr = m_action->exprQ.back();
        m_action->exprQ.pop_back();
        vl = m_action->exprQ.back();
        m_action->exprQ.pop_back();
        mulldiv_operation::muldiv_t o = m_action->muldivQ.back();
        m_action->muldivQ.pop_back();
        mulldiv_operation *f = new mulldiv_operation(vl, o, vr);
        m_action->exprQ.push_back(f);
    }
} g_push_mulldiv_binop;

struct push_function_arg : public base_action
{
    void operator()(const char *a, const char *b) const
    {
        std::string token(a,b);

        base_statement *be = m_action->exprQ.back();
        m_action->exprQ.pop_back();
        base_statement *f = m_action->funcQ.back();

        if (dynamic_cast<__function *>(f))
            dynamic_cast<__function *>(f)->push_argument(be);
    }
} g_push_function_arg;

struct push_function_name : public base_action
{
    void operator()(const char *a, const char *b) const
    {
        b--;while(*b=='(' || *b == ' ')b--;//point to function-name

        std::string fn;fn.assign(a,b-a+1);

        __function *func = new __function(fn.c_str());

        m_action->funcQ.push_back(func);
    }
} g_push_function_name;

struct push_function_expr : public base_action
{
    void operator()(const char *a, const char *b) const
    {
        std::string token(a,b);

        base_statement *func = m_action->funcQ.back();
        m_action->funcQ.pop_back();

        m_action->exprQ.push_back(func);
    }
} g_push_function_expr;

////////////////////// logical unit ////////////////////////

struct push_compare_operator : public base_action
{
    void operator()(const char *a,const char *b) const
    {
        string token(a, b);
        arithmetic_operand::cmp_t c = arithmetic_operand::cmp_t::NA;

        if (token.compare("==") == 0)
            c = arithmetic_operand::cmp_t::EQ;
        else if (token.compare("!=") == 0)
            c = arithmetic_operand::cmp_t::NE;
        else if (token.compare(">=") == 0)
            c = arithmetic_operand::cmp_t::GE;
        else if (token.compare("<=") == 0)
            c = arithmetic_operand::cmp_t::LE;
        else if (token.compare(">") == 0)
            c = arithmetic_operand::cmp_t::GT;
        else if (token.compare("<") == 0)
            c = arithmetic_operand::cmp_t::LT;
        else
            c = arithmetic_operand::cmp_t::NA;

        m_action->arithmetic_compareQ.push_back(c);
    }
} g_push_compare_operator;

struct push_logical_operator : public base_action
{
    void operator()(const char *a,const char *b) const
    {
        string token(a, b);
        logical_operand::oplog_t l = logical_operand::oplog_t::NA;

        if (token.compare("and") == 0)
            l = logical_operand::oplog_t::AND;
        else if (token.compare("or") == 0)
            l = logical_operand::oplog_t::OR;
        else
            l = logical_operand::oplog_t::NA;

        m_action->logical_compareQ.push_back(l);

    }
} g_push_logical_operator;

struct push_arithmetic_predicate : public base_action
{
    void operator()(const char *a,const char *b) const
    {
        string token(a, b);

        base_statement *vr, *vl;
        arithmetic_operand::cmp_t c = m_action->arithmetic_compareQ.back();
        m_action->arithmetic_compareQ.pop_back();
        vr = m_action->exprQ.back();
        m_action->exprQ.pop_back();
        vl = m_action->exprQ.back();
        m_action->exprQ.pop_back();

        arithmetic_operand *t = new arithmetic_operand(vl, c, vr);

        m_action->condQ.push_back(t);
    }
} g_push_arithmetic_predicate;

struct push_logical_predicate : public base_action
{

    void operator()(const char *a,const char *b) const
    {
        string token(a, b);

        base_statement *tl = 0, *tr = 0;
        logical_operand::oplog_t oplog = m_action->logical_compareQ.back();
        m_action->logical_compareQ.pop_back();

        if (m_action->condQ.empty() == false)
        {
            tr = m_action->condQ.back();
            m_action->condQ.pop_back();
        }
        if (m_action->condQ.empty() == false)
        {
            tl = m_action->condQ.back();
            m_action->condQ.pop_back();
        }

        logical_operand *f = new logical_operand(tl, oplog, tr);

        m_action->condQ.push_back(f);
    }
} g_push_logical_predicate;

struct push_column_pos : public base_action
{
    void operator()(const char *a,const char *b) const
    { 
        string token(a, b);
        variable *v;

        if (token.compare("*") == 0 || token.compare("* ")==0) //TODO space should skip in boost::spirit
            v = new variable(token.c_str(), variable::var_t::STAR_OPERATION);
         else 
            v = new variable(token.c_str(), variable::var_t::POS);

        m_action->exprQ.push_back(v);
    }

} g_push_column_pos;

struct push_projection : public base_action
{
    void operator()(const char *a,const char *b) const
    {
        string token(a, b);

        m_action->projections.get()->push_back( m_action->exprQ.back() );
        m_action->exprQ.pop_back();
    }

} g_push_projection;


/// for the schema description "mini-parser"
struct push_column : public base_action
{

    void operator()(const char *a,const char *b) const
    {
        std::string token(a,b);

        m_action->schema_columns.push_back(token); 
    }

} g_push_column;

struct push_debug_1 : public base_action
{

    void operator()(const char *a,const char *b) const
    {
        std::string token(a,b);
    }

} g_push_debug_1;

struct s3select : public grammar<s3select>
{
    private:

    actionQ m_actionQ; //TODO on heap

    scratch_area m_sca;//TODO on heap

    #define BOOST_BIND_ACTION( push_name ) boost::bind( &push_name::operator(), g_ ## push_name , _1 ,_2)
    #define ATTACH_ACTION_Q( push_name ) {(g_ ## push_name).set_action_q(&m_actionQ);}

    public:

    s3select()
    {   //TODO check option for defining action and push into list 
        ATTACH_ACTION_Q(push_from_clause);
        ATTACH_ACTION_Q(push_number);
        ATTACH_ACTION_Q(push_logical_operator);
        ATTACH_ACTION_Q(push_logical_predicate);
        ATTACH_ACTION_Q(push_compare_operator);
        ATTACH_ACTION_Q(push_arithmetic_predicate);
        ATTACH_ACTION_Q(push_addsub);
        ATTACH_ACTION_Q(push_addsub_binop);
        ATTACH_ACTION_Q(push_mulop);
        ATTACH_ACTION_Q(push_mulldiv_binop);
        ATTACH_ACTION_Q(push_function_arg);
        ATTACH_ACTION_Q(push_function_name);
        ATTACH_ACTION_Q(push_function_expr);
        ATTACH_ACTION_Q(push_float_number);
        ATTACH_ACTION_Q(push_string);
        ATTACH_ACTION_Q(push_variable);
        ATTACH_ACTION_Q(push_column_pos);
        ATTACH_ACTION_Q(push_projection);
        ATTACH_ACTION_Q(push_debug_1);

    }

    bool is_semantic()//TBD traverse and validate semantics per all nodes
    {
        base_statement * cond = m_actionQ.exprQ.back();

        return  cond->semantic();
    }
    
    std::string get_from_clause()
    {
        return m_actionQ.from_clause;
    }

    void load_schema(std::list<string> &scm)
    {
        int i = 0;
        for (auto c : scm)
            m_sca.set_column_pos(c.c_str(), i++);
    }

    base_statement* get_filter()
    {
        return m_actionQ.condQ.back();
    }

    std::list<base_statement*>  get_projections_list()
    {
        return *m_actionQ.projections.get(); //TODO return COPY(?) or to return evalaution results (list of class value{}) / return reference(?)
    }

    scratch_area* get_scratch_area()
    {
        return &m_sca;
    }

    ~s3select()
    {
        if (m_actionQ.exprQ.empty() == false)
        {
            for (auto e : m_actionQ.exprQ)
                if (e)
                {
                    e->dfs_del();
                    delete e;
                }
        }

        if (m_actionQ.condQ.empty() == false)
        {
            for (auto c : m_actionQ.condQ)
                if (c)
                {
                    c->dfs_del();
                    delete c;
                }
        }

        for (auto p : get_projections_list())
            if (p)
            {
                p->dfs_del();
                delete p;
            }
    }

    template <typename ScannerT>
    struct definition
    {
        definition(s3select const & )
        {///// s3select syntax rules and actions for building AST

            select_expr = str_p("select")  >> projections >> str_p("from") >> (s3_object)[BOOST_BIND_ACTION(push_from_clause)] >> !where_clause >> ';';
            
            projections = projection_expression >> *( ',' >> projection_expression) ;

            projection_expression = (arithmetic_expression >> str_p("as") >> alias_name) | (arithmetic_expression)[BOOST_BIND_ACTION(push_projection)]  ;

            alias_name = lexeme_d[(+alpha_p >> *digit_p)] ;


            s3_object = str_p("stdin") | object_path ; 

            object_path = "/" >> *( fs_type >> "/") >> fs_type;

            fs_type = lexeme_d[+( alnum_p | str_p(".")  | str_p("_")) ];

            where_clause = str_p("where") >> condition_expression;

            condition_expression = (arithmetic_predicate >> *(log_op[BOOST_BIND_ACTION(push_logical_operator)] >> arithmetic_predicate[BOOST_BIND_ACTION(push_logical_predicate)]));

            arithmetic_predicate = (factor >> *(arith_cmp[BOOST_BIND_ACTION(push_compare_operator)] >> factor[BOOST_BIND_ACTION(push_arithmetic_predicate)]));

            factor = (arithmetic_expression) | ('(' >> condition_expression >> ')') ; 

            arithmetic_expression = (addsub_operand >> *(addsubop_operator[BOOST_BIND_ACTION(push_addsub)] >> addsub_operand[BOOST_BIND_ACTION(push_addsub_binop)] ));

            addsub_operand = (mulldiv_operand >> *(muldiv_operator[BOOST_BIND_ACTION(push_mulop)]  >> mulldiv_operand[BOOST_BIND_ACTION(push_mulldiv_binop)] ));// this non-terminal gives precedense to  mull/div

            mulldiv_operand = arithmetic_argument | ('(' >> (arithmetic_expression) >> ')') ; 

            list_of_function_arguments = (arithmetic_expression)[BOOST_BIND_ACTION(push_function_arg)] >> *(',' >> (arithmetic_expression)[BOOST_BIND_ACTION(push_function_arg)]) ;

            function =( (variable >>  '(' )[BOOST_BIND_ACTION(push_function_name)]   >> list_of_function_arguments >> ')' ) [BOOST_BIND_ACTION(push_function_expr)];
            
            arithmetic_argument = (float_number)[BOOST_BIND_ACTION(push_float_number)] |  (number)[BOOST_BIND_ACTION(push_number)] | (column_pos)[BOOST_BIND_ACTION(push_column_pos)] | 
                                (string)[BOOST_BIND_ACTION(push_string)] |
                                (function)[BOOST_BIND_ACTION(push_debug_1)]  | (variable)[BOOST_BIND_ACTION(push_variable)] ;//function is pushed by right-term 

                       
            number = int_p;

            float_number = real_p;

            string = str_p("\"") >> *( anychar_p - str_p("\"") ) >> str_p("\"") ;

            column_pos = ('_'>>+(digit_p) ) | '*' ;

            muldiv_operator = str_p("*") | str_p("/") | str_p("^");// got precedense

            addsubop_operator = str_p("+") | str_p("-");


            arith_cmp = str_p(">=") | str_p("<=") | str_p("==") | str_p("<") | str_p(">") | str_p("!=");

            log_op = str_p("and") | str_p("or"); //TODO add NOT (unary)

            variable =  lexeme_d[(+alpha_p >> *digit_p)];
        }
    

        rule<ScannerT> variable, select_expr, s3_object, where_clause, number, float_number, string, arith_cmp, log_op, condition_expression, arithmetic_predicate, factor;
        rule<ScannerT> muldiv_operator, addsubop_operator, function, arithmetic_expression, addsub_operand, list_of_function_arguments, arithmetic_argument, mulldiv_operand;
        rule<ScannerT> fs_type,object_path;
        rule<ScannerT> projections,projection_expression,alias_name,column_pos;
        rule<ScannerT> const &start() const { return select_expr; }
    };
};

/////// handling different object types
class base_s3object
{

protected:
    scratch_area *m_sa;
    std::string m_obj_name;

public:
    base_s3object(const char *obj_name, scratch_area *m) : m_sa(m), m_obj_name(obj_name) {}
    base_s3object(scratch_area *m) : m_sa(m), m_obj_name("") {}

    virtual int getNextRow(char **) = 0; //fetch next row

    virtual ~base_s3object(){}
};

#include <boost/algorithm/string.hpp>

class csv_object : public base_s3object
{

private:
  std::vector<std::string> m_csv_lines;
  base_statement *m_where_clause;
  list<base_statement *> m_projections;
  bool m_aggr_flow = false; //TODO once per query
  int line_index;

  int getNextRow(char **tokens) //TODO add delimiter
  {                             //purpose: simple csv parser, not handling escape rules

    if (line_index >= (int)m_csv_lines.size()) return -1;
    if(m_csv_lines[line_index].empty()) return -1;
    
    char *p = (char *)m_csv_lines[line_index].c_str(); //TODO another boost splitter
    int i = 0;
    while (*p)
    {
      char *t = p;
      while (*p && (*(p) != ',')) //TODO delimiter
        p++;
      *p = 0;
      tokens[i++] = t;
      p++;
      if (*t == 10)
        break; //trimming newline
    }
    tokens[i] = 0; //last token
    line_index++;
    return i;
  }

public:
  csv_object(s3select *s3_query,std::string query,char *csv_stream) : base_s3object(s3_query->get_scratch_area())
  {

  parse_info<> info = parse(query.c_str(), *s3_query, space_p);
  auto x = info.stop;

  if (!info.full)
  {
    std::cout << "failure -->" << x << "<---" << std::endl; //TODO exception??
  }

    std::string __stream(csv_stream);
    boost::split(m_csv_lines, __stream, [](char c) { return c == '\n'; }); //splitting the stream (should be 4->128 mb)
    line_index = 0;
    m_projections = s3_query->get_projections_list();
    m_where_clause = s3_query->get_filter();

    if (m_where_clause)
      m_where_clause->traverse_and_apply(m_sa);

    for (auto p : m_projections)
      p->traverse_and_apply(m_sa);

    for (auto e : m_projections) //TODO for tests only, should be in semantic
    {
      base_statement *aggr = 0;

      if ((aggr = e->get_aggregate()) != 0)
      {
        if (aggr->is_nested_aggregate(aggr))
        {
          throw base_s3select_exception("nested aggregation function is illegal i.e. sum(...sum ...)", base_s3select_exception::s3select_exp_en_t::FATAL);
        }

        m_aggr_flow = true;
      }
    }
    if (m_aggr_flow == true)
      for (auto e : m_projections)
      {
        base_statement *skip_expr = e->get_aggregate();

        if (e->is_binop_aggregate_and_column(skip_expr))
        {
          throw base_s3select_exception("illegal expression. /select sum(c1) + c1 ..../ is not allow type of query", base_s3select_exception::s3select_exp_en_t::FATAL);
        }
      }
  }

virtual ~csv_object(){}

public:
  int getMatchRow(std::list<string> &result) //TODO virtual ? getResult
  {
    int number_of_tokens = 0;
    char *row_tokens[128]; //TODO typedef for it

    if (m_aggr_flow == true)
    {
      do
      {

        number_of_tokens = getNextRow(row_tokens);
        if (number_of_tokens < 0) //end of stream
        {
          for (auto i : m_projections)
          {
            i->set_last_call();
            result.push_back((i->eval().to_string()));
          }
          return number_of_tokens;
        }

        m_sa->update((const char **)row_tokens, number_of_tokens);

        if (!m_where_clause || m_where_clause->eval().get_num() == true)
          for (auto i : m_projections)
            i->eval();

      } while (1);
    }
    else
    {

      do
      {

        number_of_tokens = getNextRow(row_tokens);
        if (number_of_tokens < 0)
          return number_of_tokens;

        m_sa->update((const char **)row_tokens, number_of_tokens);
      } while (m_where_clause && m_where_clause->eval().get_num() == false);

      for (auto i : m_projections)
      {
        result.push_back(i->eval().to_string());
      }
    }

    return number_of_tokens; //TODO wrong
  }


int run_s3select_on_object(std::string & o_result)//,s3select &s3select_syntax)
{
  try
  {
    //csv_object my_input(&s3select_syntax, input_stream);

    do
    {
      std::list<string> result;
      int num = getMatchRow(result);

      for (std::list<string>::iterator it = result.begin(); it != result.end(); it++)
      {
        if (std::next(it) != result.end())
          {o_result.append(*it);o_result.append(",");}
        else
          {o_result.append(*it);o_result.append("\n");}
      }

      if (num < 0)
        break;

    } while (1);
  }
  catch (base_s3select_exception & e)
  {
    std::cout << e.what() << std::endl;
    return -1;
  }

  return 0;
}
};
