default: test

test:
	@echo "------------------"
	@echo " test"
	@echo "------------------"
	@go test -coverprofile=coverage.out

html:
	@echo "------------------"
	@echo " html report"
	@echo "------------------"
	@go tool cover -html=coverage.out -o coverage.html
	@open coverage.html

detail:
	@echo "------------------"
	@echo " detailed report"
	@echo "------------------"
	@gocov test | gocov report

report: test detail html
