package job

type OptFunc func(*ManagerOption)

func DBName(name string) OptFunc {
	return func(mo *ManagerOption) {
		mo.DBName = name
	}
}
