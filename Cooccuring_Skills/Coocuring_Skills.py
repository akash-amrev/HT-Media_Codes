import operator
import itertools
import csv

def getCandSkill():
                        InFile='/data/Shine/Shine_AdHoc/Output/cand_skills.csv'
                        ofile = open('/data/Shine/Shine_AdHoc/Output/cand_skills_comb.csv',"wb+")
                        with open(InFile) as inputFile:
                                reader = csv.DictReader(inputFile,delimiter=',')
                                userid_prev = ''
                                skilllist = []
                                #count = 1
                                for line in reader:
                                    if str(line['UserId']) == userid_prev or userid_prev == '':
                                        skilllist.append(str(line['Skills']))
                                        userid_prev = str(line['UserId'])
                                        #userid_sort.sort(key=lambda x:x[0])
                                        #if str(line['UserId']) == '10000036':
                                            #print(skilllist)
                                    else:
                                        #print(str(line['UserId']))
                                        #ofile.write(str(line['UserId'])
                                        
                                        elements = list(itertools.combinations(skilllist,2))
                                        #print(elements,type(elements))
                                        #print elements
                                        for e in elements :
                                            ofile.write(str(line['UserId']))
                                            ofile.write(str(e) + '\n')    
                                        skilllist = []
                                        skilllist.append(str(line['Skills']))
                                        #print(skilllist)
                                        userid_prev=str(line['UserId'])
                                        #print(userid_prev)
                                ofile.close()
                                    


def main():
    getCandSkill()

if __name__=='__main__':
    main()
    
        
