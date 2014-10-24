import clustertext as ct

some_person = {
    'title':'Data Scientist',
    'age': int('4'),
    'industry': 'Real Estate',
    'size': int('3'),
    'resume': 'data/resume/roberto-martins-resume-final.txt',
    'type': int('1')
}

some_history = [{'company': 1123, 'rating': 4}, {'company': 5349578, 'rating': 5}]
#[ history[comp_index]['company'], history[comp_index]['rating'] ]


result = ct.run_master_ranker(person = some_person, history = some_history)

